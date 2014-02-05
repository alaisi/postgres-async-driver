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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;

import com.github.pgasync.impl.PgProtocolCallbacks;
import com.github.pgasync.impl.PgProtocolStream;
import com.github.pgasync.impl.message.Authentication;
import com.github.pgasync.impl.message.CommandComplete;
import com.github.pgasync.impl.message.DataRow;
import com.github.pgasync.impl.message.ErrorResponse;
import com.github.pgasync.impl.message.Message;
import com.github.pgasync.impl.message.ReadyForQuery;
import com.github.pgasync.impl.message.RowDescription;
import com.github.pgasync.impl.message.Startup;

/**
 * Netty connection to PostgreSQL backend. Passes received messages to
 * {@link PgProtocolCallbacks}.
 * 
 * @author Antti Laisi
 */
public class NettyPgProtocolStream implements PgProtocolStream {

    final SocketAddress address;
    final EventLoopGroup group;

    PgProtocolCallbacks callbacks;
    ChannelHandlerContext ctx;

    public NettyPgProtocolStream(SocketAddress address, EventLoopGroup group) {
        this.address = address;
        this.group = group;
    }

    @Override
    public void connect(final Startup startup, PgProtocolCallbacks callbacks) {
        this.callbacks = callbacks;

        final ChannelHandler writeStartup = new ChannelInboundHandlerAdapter() {
            @Override
            public void channelActive(ChannelHandlerContext context) throws Exception {
                context.writeAndFlush(startup);
            }
        };
        new Bootstrap().group(group).channel(NioSocketChannel.class).handler(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel channel) throws Exception {
                channel.pipeline().addLast("frame-decoder",
                        new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 1, 4, -4, 0, true));
                channel.pipeline().addLast("message-decoder", new ByteBufMessageDecoder());
                channel.pipeline().addLast("message-encoder", new ByteBufMessageEncoder());
                channel.pipeline().addLast("handler", new PgProtocolHandler());
                channel.pipeline().addLast("startup", writeStartup);
            }
        }).connect(address);
    }

    @Override
    public void send(Message... messages) {
        for (Message message : messages) {
            ctx.write(message);
        }
        ctx.flush();
    }

    @Override
    public void close() {
        ctx.close();
    }

    /**
     * Protocol message handler, methods are called from Netty IO thread.
     */
    class PgProtocolHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void handlerAdded(ChannelHandlerContext context) {
            ctx = context;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
            callbacks.onThrowable(cause);
        }
        
        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            callbacks.onThrowable(new ClosedChannelException());
        }

        @Override
        public void channelRead(ChannelHandlerContext context, Object msg) {
            if (msg instanceof ErrorResponse) {
                callbacks.onErrorResponse((ErrorResponse) msg);
            } else if (msg instanceof Authentication) {
                callbacks.onAuthentication((Authentication) msg);
            } else if (msg instanceof RowDescription) {
                callbacks.onRowDescription((RowDescription) msg);
            } else if (msg instanceof DataRow) {
                callbacks.onDataRow((DataRow) msg);
            } else if (msg instanceof CommandComplete) {
                callbacks.onCommandComplete((CommandComplete) msg);
            } else if (msg instanceof ReadyForQuery) {
                callbacks.onReadyForQuery((ReadyForQuery) msg);
            }
        }
    }

}
