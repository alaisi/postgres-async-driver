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
import io.reactivex.*;
import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import org.reactivestreams.Subscriber;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

    final GenericFutureListener<Future<? super Object>> onError;
    final Queue<ObservableEmitter<? super Message>> subscribers;
    final ConcurrentMap<String,Map<String, ObservableEmitter<? super String>>> listeners = new ConcurrentHashMap<>();

    ChannelHandlerContext ctx;

    public NettyPgProtocolStream(EventLoopGroup group, SocketAddress address, boolean useSsl, boolean pipeline) {
        this.group = group;
        this.eventLoop = group.next();
        this.address = address;
        this.useSsl = useSsl; // TODO: refactor into SSLConfig with trust parameters
        this.pipeline = pipeline;
        this.subscribers = new LinkedBlockingDeque<>(); // TODO: limit pipeline queue depth
        this.onError = future -> {
            if(!future.isSuccess()) {
                subscribers.peek().onError(future.cause());
            }
        };
    }

    @Override
    public Observable<Message> connect(StartupMessage startup) {
        return Observable.create(emitter -> {

            pushSubscriber(emitter);
            new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .handler(newProtocolInitializer(newStartupHandler(startup)))
                    .connect(address)
                    .addListener(onError);

        }).flatMap(this::throwErrorResponses);
    }

    @Override
    public Observable<Message> authenticate(PasswordMessage password) {
        return Observable.create(emitter -> {

            pushSubscriber(emitter);
            write(password);

        }).flatMap(this::throwErrorResponses);
    }

    @Override
    public Observable<Message> send(Message... messages) {
        return Observable.create(emitter -> {

            if (!isConnected()) {
                emitter.onError(new IllegalStateException("Channel is closed"));
                return;
            }

            if(pipeline && !eventLoop.inEventLoop()) {
                eventLoop.submit(() -> {
                    pushSubscriber(emitter);
                    write(messages);
                });
                return;
            }

            pushSubscriber(emitter);
            write(messages);

        }).lift(throwErrorResponsesOnComplete());
    }

    @Override
    public boolean isConnected() {
        return ctx.channel().isOpen();
    }

    @Override
    public Observable<String> listen(String channel) {

        String subscriptionId = UUID.randomUUID().toString();

        return Observable.create(emitter -> {
            Map<String, ObservableEmitter<? super String>> consumers = listeners.computeIfAbsent(channel, key -> new ConcurrentHashMap<>());
            consumers.put(subscriptionId, emitter);

            emitter.setCancellable(() -> {
                if (consumers.remove(subscriptionId) == null) {
                    throw new IllegalStateException("No consumers on channel " + channel + " with id " + subscriptionId);
                }
            });
        });
    }

    @Override
    public Completable close() {
        return Completable.create(subscriber ->
                    ctx.writeAndFlush(Terminate.INSTANCE).addListener(written ->
                            ctx.close().addListener(closed -> {
            if (!closed.isSuccess()) {
                subscriber.onError(closed.cause());
                return;
            }
            subscriber.onComplete();
        })));
    }

    private void pushSubscriber(ObservableEmitter<? super Message> subscriber) {
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
        Map<String,ObservableEmitter<? super String>> consumers = listeners.get(notification.getChannel());
        if(consumers != null) {
            consumers.values().forEach(c -> c.onNext(notification.getPayload()));
        }
    }

    private Observable<Message> throwErrorResponses(Object message) {
        return message instanceof ErrorResponse
                ? Observable.error(toSqlException((ErrorResponse) message))
                : Observable.just((Message) message);
    }

    private static ObservableOperator<Message,? super Object> throwErrorResponsesOnComplete() {
        return observer -> new Observer<Object>() {

            SqlException sqlException;

            @Override
            public void onSubscribe(Disposable d) {
                observer.onSubscribe(d);
            }
            @Override
            public void onNext(Object message) {
                if (message instanceof ErrorResponse) {
                    sqlException = toSqlException((ErrorResponse) message);
                    return;
                }
                if(sqlException != null && message == ReadyForQuery.INSTANCE) {
                    observer.onError(sqlException);
                    return;
                }
                observer.onNext((Message) message);
            }
            @Override
            public void onError(Throwable e) {
                observer.onError(e);
            }
            @Override
            public void onComplete() {
                observer.onComplete();
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
                    ObservableEmitter<? super Message> subscriber = subscribers.remove();
                    subscriber.onNext((Message) msg);
                    subscriber.onComplete();
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
                Collection<Subscriber<? super Message>> unsubscribe = new LinkedList<>();
                if(subscribers.removeAll(unsubscribe)) {
                    unsubscribe.forEach(subscriber -> subscriber.onError(cause));
                }
            }
        };
    }

}
