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

import com.github.pgasync.PgConnectionPool;
import com.github.pgasync.PgProtocolStream;
import com.pgasync.ConnectionPoolBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

/**
 * {@link PgConnectionPool} that uses {@link NettyPgProtocolStream}.
 * Each pool starts a single Netty IO thread.
 *
 * @author Antti Laisi
 */
public class NettyPgConnectionPool extends PgConnectionPool {

    private final EventLoopGroup group = new NioEventLoopGroup(1);
    private final boolean useSsl;

    public NettyPgConnectionPool(ConnectionPoolBuilder.PoolProperties properties) {
        this(properties, ForkJoinPool.commonPool());
    }

    public NettyPgConnectionPool(ConnectionPoolBuilder.PoolProperties properties, Executor futuresExecutor) {
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
