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

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Arrays.asList;

/**
 * Netty connection to PostgreSQL backend.
 * 
 * @author Antti Laisi
 */
public class NettyPgProtocolStream implements PgProtocolStream {

    final EventLoopGroup group;
    final SocketAddress address;
    final boolean useSsl;

    ChannelHandlerContext ctx;
    volatile Consumer<Message> onReceive;

    public NettyPgProtocolStream(EventLoopGroup group, SocketAddress address, boolean useSsl) {
        this.group = group;
        this.address = address;
        this.useSsl = useSsl; // TODO: refactor into SSLConfig with trust parameters
    }

    @Override
    public void connect(StartupMessage startup, Consumer<List<Message>> replyTo) {
        new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(newProtocolInitializer(newStartupHandler(startup, replyTo)))
                .connect(address)
                .addListener(future -> {
                    if (!future.isSuccess()) {
                        replyTo.accept(asList(new ChannelError(future.cause())));
                    }
                });
    }

    @Override
    public void send(Message message, Consumer<List<Message>> replyTo) {
        if(!isConnected()) {
            throw new IllegalStateException("Channel is closed");
        }
        onReceive = newReplyHandler(replyTo);
        ctx.writeAndFlush(message);
    }

    @Override
    public void send(List<Message> messages, Consumer<List<Message>> replyTo) {
        if(!isConnected()) {
            throw new IllegalStateException("Channel is closed");
        }
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
                    || msg instanceof ChannelError
                    || (msg instanceof Authentication && !((Authentication) msg).isAuthenticationOk())) {
                onReceive = null;
                consumer.accept(messages);
            }
        };
    }

    ChannelInboundHandlerAdapter newStartupHandler(StartupMessage startup, Consumer<List<Message>> replyTo) {
        return new ChannelInboundHandlerAdapter() {
            @Override
            public void userEventTriggered(ChannelHandlerContext context, Object evt) throws Exception {
                if (evt instanceof SslHandshakeCompletionEvent && ((SslHandshakeCompletionEvent) evt).isSuccess()) {
                    startup(context);
                }
            }
            @Override
            public void channelActive(ChannelHandlerContext context) {
                if(useSsl) {
                    context.writeAndFlush(SSLHandshake.INSTANCE);
                } else {
                    startup(context);
                }
            }
            void startup(ChannelHandlerContext context) {
                ctx = context;
                onReceive = newReplyHandler(replyTo);
                context.writeAndFlush(startup);
                context.pipeline().remove(this);
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
                onReceive.accept((Message) msg);
            }
            @Override
            public void channelInactive(ChannelHandlerContext context) throws Exception {
                if(onReceive != null) {
                    onReceive.accept(new ChannelError("Channel state changed to inactive"));
                }
            }
            @Override
            public void exceptionCaught(ChannelHandlerContext context, Throwable cause) throws Exception {
                if(onReceive != null) {
                    onReceive.accept(new ChannelError(cause));
                }
            }
        };
    }

    static class ChannelError extends SqlException implements Message {
        ChannelError(String message) {
            super(message);
        }
        public ChannelError(Throwable cause) {
            super(cause);
        }
    }

}
