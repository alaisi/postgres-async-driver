package com.github.pgasync.impl.netty;

import com.github.pgasync.Connection;
import com.github.pgasync.ConnectionPoolBuilder;
import com.github.pgasync.impl.PgConnection;
import com.github.pgasync.impl.PgConnectionPool;
import com.github.pgasync.impl.PgProtocolStream;
import com.github.pgasync.impl.conversion.DataConverter;
import com.github.pgasync.impl.message.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import rx.Observable;
import rx.Subscriber;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;

public class EventLoopConnectionPool extends PgConnectionPool {

    private final EventLoopGroup group;
    private final EventLoop loop;
    private final ConnectionPoolBuilder.PoolProperties properties;
    private final InetSocketAddress address;
    private final DataConverter dataConverter;

    private final Queue<Connection> connections = new LinkedList<>();
    private final Queue<Subscriber<? super Connection>> subscribers = new LinkedList<>();
    private final int maxSize;
    private int size;

    public EventLoopConnectionPool(ConnectionPoolBuilder.PoolProperties properties) {
        super(properties);
        this.maxSize = properties.getPoolSize();
        this.properties = properties;
        this.group = new NioEventLoopGroup(1);
        this.loop = group.next();
        this.address = new InetSocketAddress(properties.getHostname(), properties.getPort());
        this.dataConverter = properties.getDataConverter();
    }

    @Override
    public Observable<Connection> getConnection() {
        return Observable.create(subscriber -> {
            if(loop.inEventLoop()) {
                doAcquire(subscriber);
                return;
            }
            loop.submit(() -> doAcquire(subscriber));
        });
    }

    @Override
    public void release(Connection connection) {
        if(loop.inEventLoop()) {
            doRelease(connection);
            return;
        }
        loop.submit(() -> doRelease(connection));
    }

    @Override
    public void close() throws Exception {
        loop.submit(() -> {
            for(Connection conn : connections) {
                try {
                    conn.close();
                } catch (Exception e) {
                    e.printStackTrace(); // TODO
                }
            }
        });
        loop.shutdownGracefully();
    }

    @Override
    protected PgProtocolStream openStream(InetSocketAddress address) {
        throw new UnsupportedOperationException();
    }

    private void doAcquire(Subscriber<? super Connection> subscriber) {
        Connection conn = connections.poll();
        if(conn != null) {
            subscriber.onNext(conn);
            subscriber.onCompleted();
            return;
        }

        if(size == maxSize) {
            subscribers.add(subscriber);
            return;
        }

        size++;

        ChannelFuture f = new Bootstrap()
                .group(group)
                .handler(new PgHandler())
                .channel(NioSocketChannel.class)
                .handler(newProtocolInitializer())
                .connect(address);

        f.addListener(connected -> {
                    if(!connected.isSuccess()) {
                        subscriber.onError(connected.cause());
                        return;
                    }
                    new PgConnection(new ChannelStream(f.channel()), dataConverter)
                            .connect(properties.getUsername(), properties.getPassword(), properties.getDatabase())
                            .doOnError(__ -> release(null))
                            .subscribe(subscriber::onNext, subscriber::onError, subscriber::onCompleted);
                });
    }

    private void doRelease(Connection connection) {
        boolean closed = connection == null || !((PgConnection) connection).isConnected();
        Subscriber<? super Connection> subscriber = subscribers.poll();

        if(closed) {
            size--;
        }

        if(subscriber == null && !closed) {
            connections.add(connection);
            return;
        }
        if(subscriber != null && !closed) {
            subscriber.onNext(connection);
            subscriber.onCompleted();
            return;
        }
        if(subscriber != null) {
            doAcquire(subscriber);
        }
    }

    static class ChannelStream implements PgProtocolStream {

        final Channel channel;

        ChannelStream(Channel channel) {
            this.channel = channel;
        }

        @Override
        public Observable<Message> connect(StartupMessage startup) {
            return send(startup);
        }
        @Override
        public Observable<Message> authenticate(PasswordMessage password) {
            return send(password);
        }
        @Override
        public Observable<Message> send(Message... messages) {
            return Observable.create(subscriber -> channel.writeAndFlush(tuple(subscriber, messages))
                    .addListener(f -> {
                        if(!f.isSuccess()) {
                            subscriber.onError(f.cause());
                        }
            }));
        }
        @Override
        public Observable<String> listen(String channelName) {

            String subscriptionId = UUID.randomUUID().toString();

            return Observable
                    .<String>create(subscriber ->
                            channel.pipeline().fireUserEventTriggered(new AddListener(channelName, null, subscriber)))
                    .doOnUnsubscribe(() ->
                            channel.pipeline().fireUserEventTriggered(new RemoveListener(channelName, subscriptionId)));
        }
        @Override
        public boolean isConnected() {
            return channel.isActive();
        }
        @Override
        public Observable<Void> close() {
            return Observable.create(subscriber -> {
                channel.close().addListener(f -> {
                    if(!f.isSuccess()) {
                        subscriber.onError(f.cause());
                        return;
                    }
                    subscriber.onNext(null);
                    subscriber.onCompleted();
                });
            });
        }

        static <K,V> Entry<K,V> tuple(K key, V value) {
            return new SimpleImmutableEntry<>(key, value);
        }
    }

    static ChannelInitializer<Channel> newProtocolInitializer() {
        return new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel channel) throws Exception {
                channel.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 1, 4, -4, 0, true));
                channel.pipeline().addLast(new ByteBufMessageDecoder());
                channel.pipeline().addLast(new ByteBufMessageEncoder());
                channel.pipeline().addLast(new PgHandler());
            }
        };
    }

    static class PgHandler extends ChannelDuplexHandler {

        final Queue<Subscriber<Message>> subscribers = new LinkedList<>();
        final Map<String,Map<String,Subscriber<? super String>>> listeners = new HashMap<>();

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if(evt instanceof AddListener) {
                AddListener add = (AddListener) evt;
                Map<String,Subscriber<? super String>> consumers = listeners.get(add.channel);
                if(consumers == null) {
                    listeners.put(add.channel, consumers = new HashMap<>());
                    consumers.put(add.uuid, add.subscriber);
                }
            } else if(evt instanceof RemoveListener) {
                RemoveListener rl = (RemoveListener) evt;
                listeners.get(rl.channel).remove(rl.uuid);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            Entry<Subscriber<Message>, Message[]> tuple = (Entry<Subscriber<Message>, Message[]>) msg;
            subscribers.add(tuple.getKey());

            for(Message m : tuple.getValue()) {
                ctx.write(m).addListener(f -> {
                    if(!f.isSuccess()) {
                        tuple.getKey().onError(f.cause());
                        ctx.channel().close();
                    }
                });
            }
            ctx.flush();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

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
            Collection<Subscriber<Message>> unsubscribe = new LinkedList<>();
            if(subscribers.removeAll(unsubscribe)) {
                unsubscribe.forEach(subscriber -> subscriber.onError(cause));
            }
        }

        private static boolean isCompleteMessage(Object msg) {
            return msg == ReadyForQuery.INSTANCE
                    || (msg instanceof Authentication && !((Authentication) msg).isAuthenticationOk());
        }

        private void publishNotification(NotificationResponse notification) {
            Map<String,Subscriber<? super String>> consumers = listeners.get(notification.getChannel());
            if(consumers != null) {
                consumers.values().forEach(c -> c.onNext(notification.getPayload()));
            }
        }
    }

    static class AddListener {
        final String channel;
        final String uuid;
        final Subscriber<? super String> subscriber;

        AddListener(String channel, String uuid, Subscriber<? super String> subscriber) {
            this.channel = channel;
            this.uuid = uuid;
            this.subscriber = subscriber;
        }
    }
    static class RemoveListener {
        final String channel;
        final String uuid;
        RemoveListener(String channel, String uuid) {
            this.channel = channel;
            this.uuid = uuid;
        }
    }

}
