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

/**
 * Netty connection to PostgreSQL backend.
 *
 * @author Antti Laisi
 */
public class NettyPgProtocolStream implements PgProtocolStream {

    final EventLoopGroup group;
    final EventLoop eventLoop;
    final SocketAddress address;
    final boolean useSsl;
    final boolean pipeline;
    final Bootstrap channelPipeline;

    final GenericFutureListener<Future<? super Object>> outboundErrorListener = written -> {
        if (!written.isSuccess()) {
            respondWithException(written.cause());
        }
    };
    final Queue<CompletableFuture<? super Message>> uponResponses = new LinkedBlockingDeque<>(); // TODO: limit pipeline queue depth;
    // final ConcurrentMap<String, Map<String, Subscriber<? super String>>> listeners = new ConcurrentHashMap<>();

    ChannelHandlerContext ctx;

    public NettyPgProtocolStream(EventLoopGroup group, SocketAddress address, boolean useSsl, boolean pipeline) {
        this.group = group;
        this.eventLoop = group.next();
        this.address = address;
        this.useSsl = useSsl; // TODO: refactor into SSLConfig with trust parameters
        this.pipeline = pipeline;
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
                                    .thenAccept(message -> {
                                        write(startup);
                                    })
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
        return offerRoundTrip(() -> write(password));
    }

    @Override
    public CompletableFuture<Message> send(Message... messages) {
        return offerRoundTrip(() -> write(messages));
    }

    @Override
    public boolean isConnected() {
        return ctx.channel().isOpen();
    }

    /*
        @Override
        public Observable<String> listen(String channel) {

            String subscriptionId = UUID.randomUUID().toString();

            return Observable.<String>create(subscriber -> {

                Map<String, Subscriber<? super String>> consumers = new ConcurrentHashMap<>();
                Map<String, Subscriber<? super String>> old = listeners.putIfAbsent(channel, consumers);
                consumers = old != null ? old : consumers;

                consumers.put(subscriptionId, subscriber);

            }).doOnUnsubscribe(() -> {

                Map<String, Subscriber<? super String>> consumers = listeners.get(channel);
                if (consumers == null || consumers.remove(subscriptionId) == null) {
                    throw new IllegalStateException("No consumers on channel " + channel + " with id " + subscriptionId);
                }
            });
        }
    */

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
        uponResponses.remove().completeExceptionally(th);
    }

    private void respondWithMessage(Message message) {
        if (message instanceof ErrorResponse) {
            respondWithException(toSqlException((ErrorResponse) message));
        } else {
            uponResponses.remove().complete(message);
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
        Queue<Message> queue = new LinkedList<>(Arrays.asList(messages));
        GenericFutureListener<Future<Void>> listener = new GenericFutureListener<>() {
            @Override
            public void operationComplete(Future<Void> written) {
                if (written.isSuccess()) {
                    Message message = queue.poll();
                    if (message != null) {
                        ctx.writeAndFlush(message).addListener(this);
                    }
                } else {
                    respondWithException(written.cause());
                }
            }
        };
        ctx.writeAndFlush(queue.poll()).addListener(listener);
    }

    /*
    private void publishNotification(Notification notification) {
        Map<String, Subscriber<? super String>> consumers = listeners.get(notification.getChannel());
        if (consumers != null) {
            consumers.values().forEach(c -> c.onNext(notification.getPayload()));
        }
    }
    */
    private static boolean isCompleteMessage(Object msg) {
        return msg == ReadyForQuery.INSTANCE
                || (msg instanceof Authentication && !((Authentication) msg).isAuthenticationOk());
    }

    private static SqlException toSqlException(ErrorResponse error) {
        return new SqlException(error.getLevel().name(), error.getCode(), error.getMessage());
    }

    ChannelInitializer<Channel> newProtocolInitializer() {
        return new ChannelInitializer<Channel>() {
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
            public void channelRead(ChannelHandlerContext context, Object msg) {
                if (msg instanceof Notification) {
                    // publishNotification((Notification) msg);
                } else if (isCompleteMessage(msg)) {
                    respondWithMessage((Message) msg);
                } else {
                    // No op, since incomplete message from the network
                }
            }

            @Override
            public void channelInactive(ChannelHandlerContext context) {
                exceptionCaught(context, new IOException("Channel state changed to inactive"));
            }

            @Override
            @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
            public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
                while (!uponResponses.isEmpty()) {
                    respondWithException(cause);
                }
            }
        };
    }

}
