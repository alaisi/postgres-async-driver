/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.pgasync.netty;

import com.github.pgasync.message.frontend.Bind;
import com.github.pgasync.message.frontend.Describe;
import com.pgasync.SqlException;
import com.github.pgasync.PgProtocolStream;
import com.github.pgasync.message.ExtendedQueryMessage;
import com.github.pgasync.message.Message;
import com.github.pgasync.message.backend.Authentication;
import com.github.pgasync.message.backend.BIndicators;
import com.github.pgasync.message.backend.CommandComplete;
import com.github.pgasync.message.backend.DataRow;
import com.github.pgasync.message.backend.ErrorResponse;
import com.github.pgasync.message.backend.NoticeResponse;
import com.github.pgasync.message.backend.NotificationResponse;
import com.github.pgasync.message.backend.ReadyForQuery;
import com.github.pgasync.message.backend.RowDescription;
import com.github.pgasync.message.frontend.Execute;
import com.github.pgasync.message.frontend.FIndicators;
import com.github.pgasync.message.frontend.PasswordMessage;
import com.github.pgasync.message.frontend.Query;
import com.github.pgasync.message.backend.SSLHandshake;
import com.github.pgasync.message.frontend.SSLRequest;
import com.github.pgasync.message.frontend.StartupMessage;
import com.github.pgasync.message.frontend.Terminate;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Netty messages stream to Postgres backend.
 *
 * @author Antti Laisi
 */
public class NettyPgProtocolStream implements PgProtocolStream {

    private final SocketAddress address;
    private final boolean useSsl;
    private final Bootstrap channelPipeline;
    private final Executor futuresExecutor;
    private final Charset encoding;

    private StartupMessage startupWith;

    private ChannelHandlerContext ctx;

    private final GenericFutureListener<Future<? super Object>> outboundErrorListener = written -> {
        if (!written.isSuccess()) {
            respondWithException(written.cause());
        }
    };
    private CompletableFuture<? super Message> onResponse;
    private final Map<String, Set<Consumer<String>>> subscriptions = new HashMap<>();

    private Consumer<RowDescription.ColumnDescription[]> onColumns;
    private Consumer<DataRow> onRow;
    private Consumer<CommandComplete> onAffected;

    private boolean seenReadyForQuery;
    private Message readyForQueryPendingMessage;
    private Message lastSentMessage;

    NettyPgProtocolStream(EventLoopGroup group, SocketAddress address, boolean useSsl, Charset encoding, Executor futuresExecutor) {
        this.address = address;
        this.useSsl = useSsl; // TODO: refactor into SSLConfig with trust parameters
        this.channelPipeline = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(newProtocolInitializer());
        this.encoding = encoding;
        this.futuresExecutor = futuresExecutor;
    }

    private CompletableFuture<? super Message> consumeOnResponse() {
        CompletableFuture<? super Message> wasOnResponse = onResponse;
        onResponse = null;
        return wasOnResponse;
    }

    @Override
    public CompletableFuture<Message> connect(StartupMessage startup) {
        startupWith = startup;
        return offerRoundTrip(() -> channelPipeline.connect(address).addListener(outboundErrorListener), false)
                .thenApply(this::send)
                .thenCompose(Function.identity())
                .thenApply(message -> {
                    if (message == SSLHandshake.INSTANCE) {
                        return send(startup);
                    } else {
                        return CompletableFuture.completedFuture(message);
                    }
                })
                .thenCompose(Function.identity());
    }

    @Override
    public CompletableFuture<Message> authenticate(PasswordMessage password) {
        return send(password);
    }

    @Override
    public CompletableFuture<Message> send(Message message) {
        return offerRoundTrip(() -> {
            lastSentMessage = message;
            write(message);
            if (message instanceof ExtendedQueryMessage) {
                write(FIndicators.SYNC);
            }
        });
    }

    @Override
    public CompletableFuture<Void> send(Query query, Consumer<RowDescription.ColumnDescription[]> onColumns, Consumer<DataRow> onRow, Consumer<CommandComplete> onAffected) {
        this.onColumns = onColumns;
        this.onRow = onRow;
        this.onAffected = onAffected;
        return send(query).thenAccept(readyForQuery -> {
        });
    }

    @Override
    public CompletableFuture<Integer> send(Bind bind, Describe describe, Consumer<RowDescription.ColumnDescription[]> onColumns, Consumer<DataRow> onRow) {
        this.onColumns = onColumns;
        this.onRow = onRow;
        this.onAffected = null;
        return offerRoundTrip(() -> {
            Execute execute;
            lastSentMessage = execute = new Execute();
            write(bind, describe, execute, FIndicators.SYNC);
        }).thenApply(commandComplete -> ((CommandComplete) commandComplete).getAffectedRows());
    }

    @Override
    public CompletableFuture<Integer> send(Bind bind, Consumer<DataRow> onRow) {
        this.onColumns = null;
        this.onRow = onRow;
        this.onAffected = null;
        return offerRoundTrip(() -> {
            Execute execute;
            lastSentMessage = execute = new Execute();
            write(bind, execute, FIndicators.SYNC);
        }).thenApply(commandComplete -> ((CommandComplete) commandComplete).getAffectedRows());
    }

    @Override
    public boolean isConnected() {
        return ctx.channel().isOpen();
    }

    @Override
    public Runnable subscribe(String channel, Consumer<String> onNotification) {
        subscriptions
                .computeIfAbsent(channel, ch -> new HashSet<>())
                .add(onNotification);
        return () -> subscriptions.computeIfPresent(channel, (ch, subscription) -> {
            subscription.remove(onNotification);
            if (subscription.isEmpty()) {
                return null;
            } else {
                return subscription;
            }
        });
    }

    @Override
    public CompletableFuture<Void> close() {
        CompletableFuture<Void> uponClose = new CompletableFuture<>();
        ctx.writeAndFlush(Terminate.INSTANCE).addListener(written -> {
            if (written.isSuccess()) {
                ctx.close().addListener(closed -> {
                    if (closed.isSuccess()) {
                        uponClose.completeAsync(() -> null, futuresExecutor);
                    } else {
                        Throwable th = closed.cause();
                        futuresExecutor.execute(() -> uponClose.completeExceptionally(th));
                    }
                });
            } else {
                Throwable th = written.cause();
                futuresExecutor.execute(() -> uponClose.completeExceptionally(th));
            }
        });
        return uponClose;
    }

    private void respondWithException(Throwable th) {
        onColumns = null;
        onRow = null;
        onAffected = null;
        readyForQueryPendingMessage = null;
        lastSentMessage = null;
        if (onResponse != null) {
            CompletableFuture<? super Message> uponResponse = consumeOnResponse();
            futuresExecutor.execute(() -> uponResponse.completeExceptionally(th));
        }
    }

    private void respondWithMessage(Message message) {
        if (message instanceof NotificationResponse) {
            publish((NotificationResponse) message);
        } else if (message instanceof NoticeResponse) {
            Logger.getLogger(NettyPgProtocolStream.class.getName()).log(Level.WARNING, message.toString());
        } else if (message == BIndicators.BIND_COMPLETE) {
            // op op since bulk message sequence
        } else if (message == BIndicators.PARSE_COMPLETE || message == BIndicators.CLOSE_COMPLETE) {
            readyForQueryPendingMessage = message;
        } else if (message instanceof RowDescription) {
            onColumns.accept(((RowDescription) message).getColumns());
        } else if (message == BIndicators.NO_DATA) {
            onColumns.accept(new RowDescription.ColumnDescription[]{});
        } else if (message instanceof DataRow) {
            onRow.accept((DataRow) message);
        } else if (message instanceof ErrorResponse) {
            if (seenReadyForQuery) {
                readyForQueryPendingMessage = message;
            } else {
                respondWithException(toSqlException((ErrorResponse) message));
            }
        } else if (message instanceof CommandComplete) {
            if (isSimpleQueryInProgress()) {
                onAffected.accept((CommandComplete) message);
            } else {
                // assert !isSimpleQueryInProgress() :
                // "During simple query message flow, CommandComplete message should be consumed only by dedicated callback,
                // due to possibility of multiple CommandComplete messages, one per sql clause.";
                readyForQueryPendingMessage = message;
            }
        } else if (message instanceof Authentication) {
            Authentication authentication = (Authentication) message;
            if (authentication.isAuthenticationOk()) {
                readyForQueryPendingMessage = message;
            } else {
                consumeOnResponse().completeAsync(() -> message, futuresExecutor);
            }
        } else if (message == ReadyForQuery.INSTANCE) {
            seenReadyForQuery = true;
            if (readyForQueryPendingMessage instanceof ErrorResponse) {
                respondWithException(toSqlException((ErrorResponse) readyForQueryPendingMessage));
            } else {
                onColumns = null;
                onRow = null;
                onAffected = null;
                Message response = readyForQueryPendingMessage != null ? readyForQueryPendingMessage : message;
                consumeOnResponse().completeAsync(() -> response, futuresExecutor);
            }
            readyForQueryPendingMessage = null;
        } else {
            consumeOnResponse().completeAsync(() -> message, futuresExecutor);
        }
    }

    private CompletableFuture<Message> offerRoundTrip(Runnable requestAction) {
        return offerRoundTrip(requestAction, true);
    }

    private CompletableFuture<Message> offerRoundTrip(Runnable requestAction, boolean assumeConnected) {
        CompletableFuture<Message> uponResponse = new CompletableFuture<>();
        if (!assumeConnected || isConnected()) {
            if (onResponse == null) {
                onResponse = uponResponse;
                try {
                    requestAction.run();
                } catch (Throwable th) {
                    respondWithException(th);
                }
            } else {
                futuresExecutor.execute(() -> uponResponse.completeExceptionally(new IllegalStateException("Postgres messages stream simultaneous use detected")));
            }
        } else {
            futuresExecutor.execute(() -> uponResponse.completeExceptionally(new IllegalStateException("Channel is closed")));
        }
        return uponResponse;
    }

    private void write(Message... messages) {
        for (Message message : messages) {
            ctx.write(message).addListener(outboundErrorListener);
        }
        ctx.flush();
    }

    private void publish(NotificationResponse notification) {
        Set<Consumer<String>> consumers = subscriptions.get(notification.getChannel());
        if (consumers != null) {
            consumers.forEach(c -> c.accept(notification.getPayload()));
        }
    }

    private boolean isSimpleQueryInProgress() {
        return lastSentMessage instanceof Query;
    }

    private boolean isExtendedQueryInProgress() {
        return lastSentMessage instanceof ExtendedQueryMessage;
    }

    private static SqlException toSqlException(ErrorResponse error) {
        return new SqlException(error.getLevel(), error.getCode(), error.getMessage());
    }

    private ChannelInitializer<Channel> newProtocolInitializer() {
        return new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel channel) {
                if (useSsl) {
                    channel.pipeline().addLast(newSslInitiator());
                }
                channel.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 1, 4, -4, 0, true));
                channel.pipeline().addLast(new ByteBufMessageDecoder(encoding));
                channel.pipeline().addLast(new ByteBufMessageEncoder(encoding));
                channel.pipeline().addLast(newProtocolHandler());
            }
        };
    }

    private ChannelHandler newSslInitiator() {
        return new ByteToMessageDecoder() {
            @Override
            protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
                if (in.readableBytes() >= 1) {
                    if ('S' == in.readByte()) { // SSL supported response
                        ctx.pipeline().remove(this);
                        ctx.pipeline().addFirst(
                                SslContextBuilder
                                        .forClient()
                                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                                        .build()
                                        .newHandler(ctx.alloc()));
                    } else {
                        ctx.fireExceptionCaught(new IllegalStateException("SSL required but not supported by Postgres"));
                    }
                }
            }
        };
    }

    private ChannelHandler newProtocolHandler() {
        return new ChannelInboundHandlerAdapter() {

            @Override
            public void channelActive(ChannelHandlerContext context) {
                NettyPgProtocolStream.this.ctx = context;
                if (useSsl) {
                    respondWithMessage(SSLRequest.INSTANCE);
                } else {
                    respondWithMessage(startupWith);
                }
            }

            @Override
            public void userEventTriggered(ChannelHandlerContext context, Object evt) {
                if (evt instanceof SslHandshakeCompletionEvent && ((SslHandshakeCompletionEvent) evt).isSuccess()) {
                    respondWithMessage(SSLHandshake.INSTANCE);
                }
            }

            @Override
            public void channelRead(ChannelHandlerContext context, Object message) {
                if (message instanceof Message) {
                    respondWithMessage((Message) message);
                }
            }

            @Override
            public void channelInactive(ChannelHandlerContext context) {
                exceptionCaught(context, new IOException("Channel state changed to inactive"));
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
                respondWithException(cause);
            }
        };
    }

}
