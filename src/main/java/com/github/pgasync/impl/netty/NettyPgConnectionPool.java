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

import com.github.pgasync.ConnectionPoolBuilder.PoolProperties;
import com.github.pgasync.impl.PgConnectionPool;
import com.github.pgasync.impl.PgProtocolStream;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * {@link PgConnectionPool} that uses {@link NettyPgProtocolStream}. Each pool
 * starts a single Netty IO thread.
 * 
 * @author Antti Laisi
 */
public class NettyPgConnectionPool extends PgConnectionPool {

    final EventLoopGroup group = new NioEventLoopGroup(1);
    final boolean useSsl;
    final boolean pipeline;

    public NettyPgConnectionPool(PoolProperties properties) {
        super(properties);
        useSsl = properties.getUseSsl();
        pipeline = properties.getUsePipelining();
    }

    @Override
    protected PgProtocolStream openStream(InetSocketAddress address) {
        return new NettyPgProtocolStream(group, address, useSsl, pipeline);
    }

    @Override
    public void close() throws Exception {
        super.close();
        group.shutdownGracefully().await(10, TimeUnit.SECONDS);
    }
}
