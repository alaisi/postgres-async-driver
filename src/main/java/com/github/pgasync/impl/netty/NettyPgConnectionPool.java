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
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action1;
import rx.observers.Subscribers;

import java.net.InetSocketAddress;

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
    public Observable<Void> close() {
        return super.close()
                .lift(subscriber -> Subscribers.create(
                        __ -> group.shutdownGracefully().addListener(close(subscriber, null)),
                        ex -> group.shutdownGracefully().addListener(close(subscriber, ex))));
    }

    private <T> GenericFutureListener<Future<T>> close(Subscriber<?> subscriber, Throwable exception) {
        return f -> {
            if (exception == null && !f.isSuccess()) {
                subscriber.onNext(null);
                subscriber.onCompleted();
                return;
            }
            subscriber.onError(f.isSuccess() ? exception : f.cause());
        };
    }
}
