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
import io.netty.util.concurrent.ScheduledFuture;
import rx.Observable;
import rx.Subscriber;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

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
    final int connectTimeout;

    final GenericFutureListener<Future<? super Object>> onError;
    final Queue<Subscriber<? super Message>> subscribers;
    final ConcurrentMap<String,Map<String,Subscriber<? super String>>> listeners = new ConcurrentHashMap<>();

    ChannelHandlerContext ctx;
    ScheduledFuture<?> subscriptionCleanerSchedule;

    public NettyPgProtocolStream(EventLoopGroup group, SocketAddress address, boolean useSsl, boolean pipeline, int connectTimeout) {
        this.group = group;
        this.eventLoop = group.next();
        this.address = address;
        this.useSsl = useSsl; // TODO: refactor into SSLConfig with trust parameters
        this.pipeline = pipeline;
        this.connectTimeout = connectTimeout;
        this.subscribers = new LinkedBlockingDeque<>(); // TODO: limit pipeline queue depth
        this.onError = future -> {
            if(!future.isSuccess()) {
                subscribers.peek().onError(future.cause());
            }
        };
    }

    @Override
    public Observable<Message> connect(StartupMessage startup) {
        return Observable.unsafeCreate(subscriber -> {
            pushSubscriber(subscriber);
            new Bootstrap()
                    .group(group)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout)
                    .channel(NioSocketChannel.class)
                    .handler(newProtocolInitializer(newStartupHandler(startup)))
                    .connect(address)
                    .addListener(onError);

        }).flatMap(this::throwErrorResponses);
    }

    @Override
    public Observable<Message> authenticate(PasswordMessage password) {
        return Observable.unsafeCreate(subscriber -> {

            pushSubscriber(subscriber);
            write(password);

        }).flatMap(this::throwErrorResponses);
    }

    @Override
    public Observable<Message> send(Message... messages) {
        return Observable.unsafeCreate(subscriber -> {

            if (!isConnected()) {
                subscriber.onError(new IllegalStateException("Channel is closed"));
                return;
            }

            if(pipeline && !eventLoop.inEventLoop()) {
                eventLoop.submit(() -> {
                    pushSubscriber(subscriber);
                    write(messages);
                });
                return;
            }

            pushSubscriber(subscriber);
            write(messages);

        }).lift(throwErrorResponsesOnComplete());
    }

    private void scheduleSubscriptionCleaner() {
        Runnable action = () ->
                Optional.ofNullable(subscribers.peek())
                        .filter(Subscriber::isUnsubscribed)
                        .ifPresent(s -> {
                            ctx.fireExceptionCaught(new IllegalStateException());
                            ctx.close();
                        });
        subscriptionCleanerSchedule = ctx.executor().scheduleWithFixedDelay(action, 1, 1, TimeUnit.SECONDS);
    }

    private void cancelSubscriptionCleaner() {
        subscriptionCleanerSchedule.cancel(true);
    }

    @Override
    public boolean isConnected() {
        return ctx.channel().isOpen();
    }

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

    @Override
    public Observable<Void> close() {
        return Observable.unsafeCreate(subscriber ->
                    ctx.writeAndFlush(Terminate.INSTANCE).addListener(written ->
                            ctx.close().addListener(closed -> {
            if (!closed.isSuccess()) {
                subscriber.onError(closed.cause());
                return;
            }
            cancelSubscriptionCleaner();
            subscriber.onNext(null);
            subscriber.onCompleted();
        })));
    }

    private void pushSubscriber(Subscriber<? super Message> subscriber) {
        if(!subscribers.offer(subscriber)) {
            throw new IllegalStateException("Pipelining not enabled " + subscribers.peek());
        }
    }

    private void write(Message... messages) {
        for(Message message : messages) {
            ctx.write(message).addListener(onError);
        }
        ctx.flush();
    }

    private void publishNotification(NotificationResponse notification) {
        Map<String,Subscriber<? super String>> consumers = listeners.get(notification.getChannel());
        if(consumers != null) {
            consumers.values().forEach(c -> c.onNext(notification.getPayload()));
        }
    }

    private Observable<Message> throwErrorResponses(Object message) {
        return message instanceof ErrorResponse
                ? Observable.error(toSqlException((ErrorResponse) message))
                : Observable.just((Message) message);
    }

    private static Observable.Operator<Message,? super Object> throwErrorResponsesOnComplete() {
        return subscriber -> new Subscriber<Object>(subscriber) {

            SqlException sqlException;

            @Override
            public void onNext(Object message) {
                if (message instanceof ErrorResponse) {
                    sqlException = toSqlException((ErrorResponse) message);
                    return;
                }
                if(sqlException != null && message == ReadyForQuery.INSTANCE) {
                    subscriber.onError(sqlException);
                    return;
                }
                subscriber.onNext((Message) message);
            }
            @Override
            public void onError(Throwable e) {
                subscriber.onError(e);
            }
            @Override
            public void onCompleted() {
                subscriber.onCompleted();
            }
        };
    }

    private static boolean isCompleteMessage(Object msg) {
        return msg == ReadyForQuery.INSTANCE
                || (msg instanceof Authentication && !((Authentication) msg).isAuthenticationOk());
    }

    private static SqlException toSqlException(ErrorResponse error) {
        return new SqlException(error.getLevel().name(), error.getCode(), error.getMessage());
    }

    ChannelInboundHandlerAdapter newStartupHandler(StartupMessage startup) {
        return new ChannelInboundHandlerAdapter() {
            @Override
            public void channelActive(ChannelHandlerContext context) {
                NettyPgProtocolStream.this.ctx = context;
                scheduleSubscriptionCleaner();

                if(useSsl) {
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

    ChannelInitializer<Channel> newProtocolInitializer(ChannelHandler onActive) {
        return new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel channel) throws Exception {
                if(useSsl) {
                    channel.pipeline().addLast(newSslInitiator());
                }
                channel.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 1, 4, -4, 0, true));
                channel.pipeline().addLast(new ByteBufMessageDecoder());
                channel.pipeline().addLast(new ByteBufMessageEncoder());
                channel.pipeline().addLast(newProtocolHandler());
                channel.pipeline().addLast(onActive);
            }
        };
    }

    ChannelHandler newSslInitiator() {
        return new ByteToMessageDecoder() {
            @Override
            protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
                if(in.readableBytes() < 1) {
                    return;
                }
                if('S' != in.readByte()) {
                    ctx.fireExceptionCaught(new IllegalStateException("SSL required but not supported by backend server"));
                    return;
                }
                ctx.pipeline().remove(this);
                ctx.pipeline().addFirst(
                        SslContextBuilder
                                .forClient()
                                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                                .build()
                                .newHandler(ctx.alloc()));
            }
        };
    }

    ChannelHandler newProtocolHandler() {
        return new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext context, Object msg) throws Exception {

                if(msg instanceof NotificationResponse) {
                    publishNotification((NotificationResponse) msg);
                    return;
                }

                if(isCompleteMessage(msg)) {
                    Subscriber<? super Message> subscriber = subscribers.remove();
                    subscriber.onNext((Message) msg);
                    subscriber.onCompleted();
                    return;
                }

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
