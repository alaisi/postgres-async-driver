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

import com.github.pgasync.impl.PgProtocolCallbacks;
import com.github.pgasync.impl.PgProtocolStream;
import com.github.pgasync.impl.message.Authentication;
import com.github.pgasync.impl.message.Message;
import com.github.pgasync.impl.message.ReadyForQuery;
import com.github.pgasync.impl.message.StartupMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Netty connection to PostgreSQL backend.
 * 
 * @author Antti Laisi
 */
public class NettyPgProtocolStream implements PgProtocolStream {

    final SocketAddress address;
    final EventLoopGroup group;

    ChannelHandlerContext ctx;
    volatile Consumer<Message> onReceive;

    public NettyPgProtocolStream(SocketAddress address, EventLoopGroup group) {
        this.address = address;
        this.group = group;
    }

    @Override
    public void connect(StartupMessage startup, Consumer<List<Message>> replyTo) {
        ChannelHandler onActive = new ChannelInboundHandlerAdapter() {
            @Override
            public void channelActive(ChannelHandlerContext context) throws Exception {
                ctx = context;
                onReceive = newReplyHandler(replyTo);
                context.writeAndFlush(startup);
            }
        };
        new Bootstrap().group(group).channel(NioSocketChannel.class).handler(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel channel) throws Exception {
                channel.pipeline().addLast("frame-decoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 1, 4, -4, 0, true));
                channel.pipeline().addLast("message-decoder", new ByteBufMessageDecoder());
                channel.pipeline().addLast("message-encoder", new ByteBufMessageEncoder());
                channel.pipeline().addLast("message-handler", new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                        onReceive.accept((Message) msg);
                    }
                });
                channel.pipeline().addLast(onActive);
            }
        }).connect(address);
    }

    @Override
    public void send(Message message, Consumer<List<Message>> replyTo) {
        onReceive = newReplyHandler(replyTo);
        ctx.writeAndFlush(message);
    }

    @Override
    public void send(List<Message> messages, Consumer<List<Message>> replyTo) {
        onReceive = newReplyHandler(replyTo);
        messages.forEach(ctx::write);
        ctx.flush();
    }

    @Override
    public boolean isConnected() {
        return ctx.channel().isOpen();
    }

    @Override
    public void close() {
        ctx.close();
    }

    Consumer<Message> newReplyHandler(Consumer<List<Message>> consumer) {
        List<Message> messages = new ArrayList<>();
        return msg -> {
            messages.add(msg);
            if(msg instanceof ReadyForQuery
                    || (msg instanceof Authentication && !((Authentication) msg).isAuthenticationOk())) {
                onReceive = null;
                consumer.accept(messages);
            }
        };
    }

}
