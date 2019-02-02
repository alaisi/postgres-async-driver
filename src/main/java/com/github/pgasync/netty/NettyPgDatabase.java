package com.github.pgasync.netty;

import com.github.pgasync.PgDatabase;
import com.github.pgasync.PgProtocolStream;
import com.pgasync.ConnectibleBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

public class NettyPgDatabase extends PgDatabase {

    private final EventLoopGroup group = new NioEventLoopGroup(1);
    private final boolean useSsl;

    public NettyPgDatabase(ConnectibleBuilder.ConnectibleProperties properties) {
        this(properties, ForkJoinPool.commonPool());
    }

    public NettyPgDatabase(ConnectibleBuilder.ConnectibleProperties properties, Executor futuresExecutor) {
        super(properties, futuresExecutor);
        useSsl = properties.getUseSsl();
    }

    @Override
    protected PgProtocolStream openStream(InetSocketAddress address) {
        return new NettyPgProtocolStream(group, address, useSsl, encoding, futuresExecutor);
    }

    @Override
    public CompletableFuture<Void> close() {
        return super.close()
                .thenAccept(v -> group.shutdownGracefully().awaitUninterruptibly(10, TimeUnit.SECONDS));
    }

}
