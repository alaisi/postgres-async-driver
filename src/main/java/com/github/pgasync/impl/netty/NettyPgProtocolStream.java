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
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import rx.Observable;
import rx.Subscriber;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

/**
 * Netty connection to PostgreSQL backend.
 * 
 * @author Antti Laisi
 */
public class NettyPgProtocolStream implements PgProtocolStream {

    final EventLoopGroup group;
    final SocketAddress address;
    final boolean useSsl;

    final GenericFutureListener<Future<? super Object>> onError;
    final Queue<Subscriber<? super Message>> subscribers;
    final ConcurrentMap<String,Map<String,Consumer<String>>> listeners = new ConcurrentHashMap<>();

    ChannelHandlerContext ctx;

    public NettyPgProtocolStream(EventLoopGroup group, SocketAddress address, boolean useSsl, boolean pipeline) {
        this.group = group;
        this.address = address;
        this.useSsl = useSsl; // TODO: refactor into SSLConfig with trust parameters
        this.subscribers = pipeline ? new LinkedBlockingDeque<>() : new ArrayBlockingQueue<>(1);
        this.onError = future -> {
            if(!future.isSuccess()) {
                subscribers.peek().onError(future.cause());
            }
        };
    }

    @Override
    public Observable<Message> connect(StartupMessage startup) {
        return protocolObservable(subscriber -> {

            pushSubscriber(subscriber);
            new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .handler(newProtocolInitializer(newStartupHandler(startup)))
                    .connect(address)
                    .addListener(onError);
        });
    }

    @Override
    public Observable<Message> send(Message... messages) {
        return protocolObservable(subscriber -> {

            if (!isConnected()) {
                subscriber.onError(new IllegalStateException("Channel is closed"));
                return;
            }

            pushSubscriber(subscriber);
            write(messages);
        });
    }

    @Override
    public boolean isConnected() {
        return ctx.channel().isOpen();
    }

    @Override
    public String registerNotificationHandler(String channel, Consumer<String> onNotification) {
        Map<String,Consumer<String>> consumers = new ConcurrentHashMap<>();
        Map<String,Consumer<String>> old = listeners.putIfAbsent(channel, consumers);
        consumers = old != null ? old : consumers;

        String token = UUID.randomUUID().toString();
        consumers.put(token, onNotification);
        return token;
    }

    @Override
    public void unRegisterNotificationHandler(String channel, String unlistenToken) {
        Map<String,Consumer<String>> consumers = listeners.get(channel);
        if(consumers == null || consumers.remove(unlistenToken) == null) {
            throw new IllegalStateException("No consumers on channel " + channel + " with token " + unlistenToken);
        }
    }

    @Override
    public void close() {
        ctx.close();
    }

    private void pushSubscriber(Subscriber<? super Message> subscriber) {
        if(!subscribers.offer(subscriber)) {
            throw new IllegalStateException("Pipelining not enabled");
        }
    }

    private void write(Message... messages) {
        for(Message message : messages) {
            ctx.write(message).addListener(onError);
        }
        ctx.flush();
    }

    private void publishNotification(NotificationResponse notification) {
        Map<String,Consumer<String>> consumers = listeners.get(notification.getChannel());
        if(consumers != null) {
            consumers.values().forEach(c -> c.accept(notification.getPayload()));
        }
    }

    private static <T> Observable<T> protocolObservable(Observable.OnSubscribe<T> onSubscribe) {
        return Observable.create(onSubscribe)
                .filter(msg -> {
                    if (msg instanceof ErrorResponse) {
                        ErrorResponse error = (ErrorResponse) msg;
                        throw new SqlException(error.getLevel().name(), error.getCode(), error.getMessage());
                    }
                    return true;
                });
    }

    private static boolean isCompleteMessage(Object msg) {
        return msg instanceof ReadyForQuery
                || (msg instanceof Authentication && !((Authentication) msg).isAuthenticationOk());
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
                        SslContext.newClientContext(InsecureTrustManagerFactory.INSTANCE)
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
                Collection<Subscriber<? super Message>> unsubscribe = new LinkedList<>();
                if(subscribers.removeAll(unsubscribe)) {
                    unsubscribe.forEach(subscriber -> subscriber.onError(cause));
                }
            }
        };
    }

}
