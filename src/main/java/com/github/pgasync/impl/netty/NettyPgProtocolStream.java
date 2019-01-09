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

package com.github.pgasync.impl.netty;

import com.github.pgasync.SqlException;
import com.github.pgasync.impl.PgProtocolStream;
import com.github.pgasync.impl.message.*;
import com.github.pgasync.impl.message.backend.Authentication;
import com.github.pgasync.impl.message.backend.CommandComplete;
import com.github.pgasync.impl.message.backend.DataRow;
import com.github.pgasync.impl.message.backend.ErrorResponse;
import com.github.pgasync.impl.message.backend.NoticeResponse;
import com.github.pgasync.impl.message.backend.NotificationResponse;
import com.github.pgasync.impl.message.backend.ReadyForQuery;
import com.github.pgasync.impl.message.backend.RowDescription;
import com.github.pgasync.impl.message.frontend.Execute;
import com.github.pgasync.impl.message.frontend.FIndicatorss;
import com.github.pgasync.impl.message.frontend.PasswordMessage;
import com.github.pgasync.impl.message.frontend.Query;
import com.github.pgasync.impl.message.backend.SSLHandshake;
import com.github.pgasync.impl.message.frontend.SSLRequest;
import com.github.pgasync.impl.message.frontend.StartupMessage;
import com.github.pgasync.impl.message.frontend.Terminate;
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
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Netty messages stream to Postgres backend.
 *
 * @author Antti Laisi
 */
public class NettyPgProtocolStream implements PgProtocolStream {

    final EventLoopGroup group;
    final EventLoop eventLoop;
    final SocketAddress address;
    final boolean useSsl;
    final Bootstrap channelPipeline;

    ChannelHandlerContext ctx;

    final GenericFutureListener<Future<? super Object>> outboundErrorListener = written -> {
        if (!written.isSuccess()) {
            respondWithException(written.cause());
        }
    };
    final Queue<CompletableFuture<? super Message>> uponResponses = new LinkedBlockingDeque<>(Integer.valueOf(System.getProperty("pg.request.pipeline.length", "1")));
    final Map<String, Set<Consumer<String>>> subscriptions = new HashMap<>();

    Consumer<RowDescription.ColumnDescription[]> onColumns;
    Consumer<DataRow> onRow;
    Consumer<CommandComplete> onAffected;

    Message readyForQueryPendingMessage;

    public NettyPgProtocolStream(EventLoopGroup group, SocketAddress address, boolean useSsl) {
        this.group = group;
        this.eventLoop = group.next();
        this.address = address;
        this.useSsl = useSsl; // TODO: refactor into SSLConfig with trust parameters
        this.channelPipeline = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(newProtocolInitializer());
    }

    @Override
    public CompletableFuture<Message> connect(StartupMessage startup) {
        return offerRoundTrip(() ->
                channelPipeline.connect(address).addListener(connected -> {
                    if (connected.isSuccess()) {
                        if (useSsl) {
                            send(SSLRequest.INSTANCE)
                                    .thenAccept(sslHandshake -> write(startup))
                                    .exceptionally(th -> {
                                        respondWithException(th);
                                        return null;
                                    });
                        } else {
                            write(startup);
                        }
                    } else {
                        respondWithException(connected.cause());
                    }
                })
        );
    }

    @Override
    public CompletableFuture<Message> authenticate(PasswordMessage password) {
        return send(password);
    }

    @Override
    public CompletableFuture<Message> send(Message message) {
        return offerRoundTrip(() -> write(message));
    }

    @Override
    public CompletableFuture<Void> send(Query query, Consumer<RowDescription.ColumnDescription[]> onColumns, Consumer<DataRow> onRow, Consumer<CommandComplete> onAffected) {
        this.onColumns = onColumns;
        this.onRow = onRow;
        this.onAffected = onAffected;
        return send(query).thenAccept(readyForQuery -> {});
    }

    @Override
    public CompletableFuture<Integer> send(Execute execute, Consumer<DataRow> onRow) {
        this.onColumns = null;
        this.onRow = onRow;
        this.onAffected = null;
        return send(execute).thenApply(commandComplete -> ((CommandComplete) commandComplete).getAffectedRows());
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
                        uponClose.complete(null);
                    } else {
                        uponClose.completeExceptionally(closed.cause());
                    }
                });
            } else {
                uponClose.completeExceptionally(written.cause());
            }
        });
        return uponClose;
    }

    private void respondWithException(Throwable th) {
        onColumns = null;
        onRow = null;
        onAffected = null;
        uponResponses.remove().completeExceptionally(th);
    }

    private void respondWithMessage(Message message) {
        if (message instanceof NotificationResponse) {
            publish((NotificationResponse) message);
        } else if (message instanceof NoticeResponse) {
            Logger.getLogger(NettyPgProtocolStream.class.getName()).log(Level.WARNING, message.toString());
        } else if (message instanceof RowDescription) {
            if (isSimpleQueryInProgress()) {
                onColumns.accept(((RowDescription) message).getColumns());
            } else {
                uponResponses.remove().complete(message);
            }
        } else if (message instanceof DataRow) {
            onRow.accept((DataRow) message);
        } else if (message instanceof CommandComplete && isSimpleQueryInProgress()) {
            onAffected.accept((CommandComplete) message);
        } else if (message instanceof ErrorResponse) {
            readyForQueryPendingMessage = message;
        } else if (message instanceof CommandComplete) {
            // assert !isSimpleQueryInProgress() :
            // "During simple query message flow, CommandComplete message should be consumed only by dedicated callback, due to possibility of multiple CommandComplete messages, one per sql clause.";
            readyForQueryPendingMessage = message;
            write(FIndicatorss.SYNC);
        } else if (message instanceof Authentication && ((Authentication) message).isAuthenticationOk()) {
            readyForQueryPendingMessage = message;
        } else if (message == ReadyForQuery.INSTANCE) {
            if (readyForQueryPendingMessage instanceof ErrorResponse) {
                respondWithException(toSqlException((ErrorResponse) readyForQueryPendingMessage));
            } else {
                onColumns = null;
                onRow = null;
                onAffected = null;
                uponResponses.remove().complete(readyForQueryPendingMessage != null ? readyForQueryPendingMessage : message);
            }
            readyForQueryPendingMessage = null;
        }
    }

    private CompletableFuture<Message> offerRoundTrip(Runnable requestAction) {
        CompletableFuture<Message> uponResponse = new CompletableFuture<>();
        if (isConnected()) {
            if (uponResponses.offer(uponResponse)) {
                try {
                    requestAction.run();
                } catch (Throwable th) {
                    respondWithException(th);
                }
            } else {
                uponResponse.completeExceptionally(new IllegalStateException("Postgres requests queue is full"));
            }
        } else {
            uponResponse.completeExceptionally(new IllegalStateException("Channel is closed"));
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
        return onColumns != null;
    }

    private static SqlException toSqlException(ErrorResponse error) {
        return new SqlException(error.getLevel().name(), error.getCode(), error.getMessage());
    }

    ChannelInitializer<Channel> newProtocolInitializer() {
        return new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel channel) {
                if (useSsl) {
                    channel.pipeline().addLast(newSslInitiator());
                }
                channel.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 1, 4, -4, 0, true));
                channel.pipeline().addLast(new ByteBufMessageDecoder());
                channel.pipeline().addLast(new ByteBufMessageEncoder());
                channel.pipeline().addLast(newProtocolHandler());
            }
        };
    }

    ChannelHandler newSslInitiator() {
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
                        ctx.fireExceptionCaught(new IllegalStateException("SSL required but not supported by backend server"));
                    }
                }
            }
        };
    }

    ChannelHandler newProtocolHandler() {
        return new ChannelInboundHandlerAdapter() {


            @Override
            public void channelActive(ChannelHandlerContext context) {
                NettyPgProtocolStream.this.ctx = context;
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
                while (!uponResponses.isEmpty()) {
                    respondWithException(cause);
                }
            }
        };
    }

}
