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

import com.github.pgasync.impl.PgProtocolStream;
import com.github.pgasync.impl.message.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.flow.FlowControlHandler;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import rx.Emitter;
import rx.Emitter.BackpressureMode;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action1;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

import static com.github.pgasync.impl.netty.ProtocolUtils.asSqlException;
import static com.github.pgasync.impl.netty.ProtocolUtils.isCompleteMessage;

/**
 * Netty connection to PostgreSQL backend.
 *
 * @author Antti Laisi
 */
public class NettyPgProtocolStream implements PgProtocolStream {
    private final EventLoopGroup group;
    private final EventLoop eventLoop;
    private final SocketAddress address;
    private final boolean useSsl;
    private final boolean pipeline;
    private final int connectTimeout;

    private final GenericFutureListener<Future<? super Object>> onError;
    private final Queue<Emitter<Message>> subscribers = new LinkedBlockingDeque<>(); // TODO: limit pipeline queue depth
    private final ConcurrentMap<String, Map<String, Subscriber<? super String>>> listeners = new ConcurrentHashMap<>();

    private ChannelHandlerContext ctx;

    NettyPgProtocolStream(EventLoopGroup group, SocketAddress address, boolean useSsl, boolean pipeline, int connectTimeout) {
        this.group = group;
        this.eventLoop = group.next();
        this.address = address;
        this.useSsl = useSsl; // TODO: refactor into SSLConfig with trust parameters
        this.pipeline = pipeline;
        this.connectTimeout = connectTimeout;
        this.onError = future -> {
            if (!future.isSuccess()) {
                subscribers.peek().onError(future.cause());
            }
        };
    }

    @Override
    public Observable<Message> connect(StartupMessage startup) {
        Action1<Emitter<Message>> action = emitter -> {
            pushSubscriber(emitter);
            new Bootstrap()
                    .group(group)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout)
                    .channel(NioSocketChannel.class)
                    .handler(newProtocolInitializer(newStartupHandler(startup)))
                    .connect(address)
                    .addListener(onError);
        };

        return Observable
                .create(action, BackpressureMode.BUFFER)
                .flatMap(this::throwErrorResponses);
    }

    @Override
    public Observable<Message> authenticate(PasswordMessage password) {
        Action1<Emitter<Message>> action = emitter -> {
            pushSubscriber(emitter);
            write(password);
        };

        return Observable
                .create(action, BackpressureMode.BUFFER)
                .flatMap(this::throwErrorResponses);
    }

    @Override
    public Observable<Message> send(Message... messages) {
        Consumer<BackpressuredEmitter> action = emitter -> {
            if (!isConnected()) {
                emitter.onError(new IllegalStateException("Channel is closed"));
                return;
            }

            emitter.setCancellation(() -> {
                ctx.channel().config().setAutoRead(true);
                if (!emitter.completed())
                    ctx.close();
            });

            if (pipeline && !eventLoop.inEventLoop()) {
                eventLoop.submit(() -> {
                    pushSubscriber(emitter);
                    write(messages);
                });
                return;
            }

            pushSubscriber(emitter);
            write(messages);
        };

        return Observable.unsafeCreate(BackpressuredEmitter.create(action, ctx));
    }

    @Override
    public boolean isConnected() {
        return ctx.channel().isOpen();
    }

    @Override
    public Observable<String> listen(String channel) {
        String subscriptionId = UUID.randomUUID().toString();

        return Observable.<String>unsafeCreate(subscriber -> {
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

    @Override
    public Observable<Void> close() {
        return Observable.unsafeCreate(subscriber ->
                ctx.writeAndFlush(Terminate.INSTANCE).addListener(written ->
                        ctx.close().addListener(closed -> {
                            if (!closed.isSuccess()) {
                                subscriber.onError(closed.cause());
                                return;
                            }
                            subscriber.onNext(null);
                            subscriber.onCompleted();
                        }))
        );
    }

    private void pushSubscriber(Emitter<Message> subscriber) {
        if (!subscribers.offer(subscriber)) {
            throw new IllegalStateException("Pipelining not enabled " + subscribers.peek());
        }
    }

    private void write(Message... messages) {
        for (Message message : messages) {
            ctx.write(message).addListener(onError);
        }
        ctx.flush();
    }

    private void publishNotification(NotificationResponse notification) {
        Map<String, Subscriber<? super String>> consumers = listeners.get(notification.getChannel());
        if (consumers != null) {
            consumers.values().forEach(c -> c.onNext(notification.getPayload()));
        }
    }

    private Observable<Message> throwErrorResponses(Object msg) {
        Message message = (Message) msg;
        return asSqlException(message)
                .<Observable<Message>>map(Observable::error)
                .orElseGet(() -> Observable.just(message));
    }

    private ChannelInboundHandlerAdapter newStartupHandler(StartupMessage startup) {
        return new ChannelInboundHandlerAdapter() {
            @Override
            public void channelActive(ChannelHandlerContext context) {
                NettyPgProtocolStream.this.ctx = context;

                if (useSsl) {
                    write(SSLHandshake.INSTANCE);
                    return;
                }
                write(startup);
                context.pipeline().remove(this);
            }

            @Override
            public void userEventTriggered(ChannelHandlerContext context, Object evt) throws Exception {
                if (evt instanceof SslHandshakeCompletionEvent && ((SslHandshakeCompletionEvent) evt).isSuccess()) {
                    write(startup);
                    context.pipeline().remove(this);
                }
            }
        };
    }

    private ChannelInitializer<Channel> newProtocolInitializer(ChannelHandler onActive) {
        return new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel channel) throws Exception {
                if (useSsl) {
                    channel.pipeline().addLast(newSslInitiator());
                }
                channel.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 1, 4, -4, 0, true));
                channel.pipeline().addLast(new ByteBufMessageDecoder());
                channel.pipeline().addLast(new ByteBufMessageEncoder());
                channel.pipeline().addLast(new FlowControlHandler(true));
                channel.pipeline().addLast(newProtocolHandler());
                channel.pipeline().addLast(onActive);
            }
        };
    }

    private ChannelHandler newSslInitiator() {
        return new ByteToMessageDecoder() {
            @Override
            protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
                if (in.readableBytes() < 1) {
                    return;
                }
                if ('S' != in.readByte()) {
                    ctx.fireExceptionCaught(new IllegalStateException("SSL required but not supported by backend server"));
                    return;
                }
                ctx.pipeline().remove(this);
                ctx.pipeline().addFirst(
                        SslContextBuilder
                                .forClient()
                                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                                .build()
                                .newHandler(ctx.alloc())
                );
            }
        };
    }

    private ChannelHandler newProtocolHandler() {
        return new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext context, Object msg) throws Exception {
                Message message = (Message) msg;

                if (message instanceof ReadyForQuery)
                    ctx.channel().config().setAutoRead(true);
                else if (message instanceof RowDescription)
                    ctx.channel().config().setAutoRead(false);

                if (message instanceof NotificationResponse) {
                    publishNotification((NotificationResponse) message);
                } else if (isCompleteMessage(message)) {
                    Emitter<Message> subscriber = subscribers.remove();
                    subscriber.onNext((Message) msg);
                    subscriber.onCompleted();
                } else
                    subscribers.peek().onNext((Message) msg);
            }

            @Override
            public void channelInactive(ChannelHandlerContext context) throws Exception {
                exceptionCaught(context, new IOException("Channel state changed to inactive"));
            }

            @Override
            @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
            public void exceptionCaught(ChannelHandlerContext context, Throwable cause) throws Exception {
                if (!isConnected()) {
                    subscribers.forEach(subscriber -> subscriber.onError(cause));
                    subscribers.clear();
                } else
                    Optional.ofNullable(subscribers.poll()).ifPresent(s -> s.onError(cause));
            }
        };
    }

}
