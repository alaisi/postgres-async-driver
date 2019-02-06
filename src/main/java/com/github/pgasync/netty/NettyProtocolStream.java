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

import com.github.pgasync.PgProtocolStream;
import com.github.pgasync.message.Message;
import com.github.pgasync.message.backend.SSLHandshake;
import com.github.pgasync.message.frontend.SSLRequest;
import com.github.pgasync.message.frontend.StartupMessage;
import com.github.pgasync.message.frontend.Terminate;
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

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * Netty messages stream to Postgres backend.
 *
 * @author Antti Laisi
 */
public class NettyProtocolStream extends PgProtocolStream {

    private final Bootstrap channelPipeline;
    private StartupMessage startupWith;
    private ChannelHandlerContext ctx;

    private final GenericFutureListener<Future<? super Object>> outboundErrorListener = written -> {
        if (!written.isSuccess()) {
            respondWithException(written.cause());
        }
    };

    public NettyProtocolStream(EventLoopGroup group, SocketAddress address, boolean useSsl, Charset encoding, Executor futuresExecutor) {
        super(address, useSsl, encoding, futuresExecutor);
        this.channelPipeline = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(newProtocolInitializer());
    }

    @Override
    public CompletableFuture<Message> connect(StartupMessage startup) {
        startupWith = startup;
        return offerRoundTrip(() -> channelPipeline.connect(address).addListener(outboundErrorListener), false)
                .thenApply(this::send)
                .thenCompose(Function.identity())
                .thenApply(message -> {
                    if (message == SSLHandshake.INSTANCE) {
                        return send(startup);
                    } else {
                        return CompletableFuture.completedFuture(message);
                    }
                })
                .thenCompose(Function.identity());
    }

    @Override
    public boolean isConnected() {
        return ctx.channel().isOpen();
    }

    @Override
    public CompletableFuture<Void> close() {
        CompletableFuture<Void> uponClose = new CompletableFuture<>();
        ctx.writeAndFlush(Terminate.INSTANCE).addListener(written -> {
            if (written.isSuccess()) {
                ctx.close().addListener(closed -> {
                    if (closed.isSuccess()) {
                        uponClose.completeAsync(() -> null, futuresExecutor);
                    } else {
                        Throwable th = closed.cause();
                        futuresExecutor.execute(() -> uponClose.completeExceptionally(th));
                    }
                });
            } else {
                Throwable th = written.cause();
                futuresExecutor.execute(() -> uponClose.completeExceptionally(th));
            }
        });
        return uponClose;
    }

    @Override
    protected void write(Message... messages) {
        for (Message message : messages) {
            ctx.write(message).addListener(outboundErrorListener);
        }
        ctx.flush();
    }

    private ChannelInitializer<Channel> newProtocolInitializer() {
        return new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel channel) {
                if (useSsl) {
                    channel.pipeline().addLast(newSslInitiator());
                }
                channel.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 1, 4, -4, 0, true));
                channel.pipeline().addLast(new NettyMessageDecoder(encoding));
                channel.pipeline().addLast(new NettyMessageEncoder(encoding));
                channel.pipeline().addLast(newProtocolHandler());
            }
        };
    }

    private ChannelHandler newSslInitiator() {
        return new ByteToMessageDecoder() {
            @Override
            protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
                if (in.readableBytes() >= 1) {
                    if ('S' == in.readByte()) { // SSL supported response
                        ctx.pipeline().remove(this);
                        ctx.pipeline().addFirst(
                                SslContextBuilder
                                        .forClient()
                                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                                        .build()
                                        .newHandler(ctx.alloc()));
                    } else {
                        ctx.fireExceptionCaught(new IllegalStateException("SSL required but not supported by Postgres"));
                    }
                }
            }
        };
    }

    private ChannelHandler newProtocolHandler() {
        return new ChannelInboundHandlerAdapter() {

            @Override
            public void channelActive(ChannelHandlerContext context) {
                NettyProtocolStream.this.ctx = context;
                if (useSsl) {
                    respondWithMessage(SSLRequest.INSTANCE);
                } else {
                    respondWithMessage(startupWith);
                }
            }

            @Override
            public void userEventTriggered(ChannelHandlerContext context, Object evt) {
                if (evt instanceof SslHandshakeCompletionEvent && ((SslHandshakeCompletionEvent) evt).isSuccess()) {
                    respondWithMessage(SSLHandshake.INSTANCE);
                }
            }

            @Override
            public void channelRead(ChannelHandlerContext context, Object message) {
                if (message instanceof Message) {
                    respondWithMessage((Message) message);
                }
            }

            @Override
            public void channelInactive(ChannelHandlerContext context) {
                exceptionCaught(context, new IOException("Channel state changed to inactive"));
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
                respondWithException(cause);
            }
        };
    }

}
