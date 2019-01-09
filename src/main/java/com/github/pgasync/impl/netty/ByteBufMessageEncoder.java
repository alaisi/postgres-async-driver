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

import com.github.pgasync.impl.io.*;
import com.github.pgasync.impl.io.frontend.BindEncoder;
import com.github.pgasync.impl.io.frontend.CloseEncoder;
import com.github.pgasync.impl.io.frontend.DescribeEncoder;
import com.github.pgasync.impl.io.frontend.ExecuteEncoder;
import com.github.pgasync.impl.io.frontend.FIndicatorsEncoder;
import com.github.pgasync.impl.io.frontend.ParseEncoder;
import com.github.pgasync.impl.io.frontend.PasswordMessageEncoder;
import com.github.pgasync.impl.io.frontend.QueryEncoder;
import com.github.pgasync.impl.io.frontend.SSLRequestEncoder;
import com.github.pgasync.impl.io.frontend.StartupMessageEncoder;
import com.github.pgasync.impl.io.frontend.TerminateEncoder;
import com.github.pgasync.impl.message.Message;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Encodes Postgres protocol V11 messages to bytes.
 *
 * @author Antti Laisi
 */
public class ByteBufMessageEncoder extends MessageToByteEncoder<Message> {

    private static final Map<Class<?>, Encoder<?>> ENCODERS = Set.of(
            new SSLRequestEncoder(),
            new StartupMessageEncoder(),
            new PasswordMessageEncoder(),
            new QueryEncoder(),
            new ParseEncoder(),
            new BindEncoder(),
            new DescribeEncoder(),
            new ExecuteEncoder(),
            new CloseEncoder(),
            new FIndicatorsEncoder(),
            new TerminateEncoder()
    ).stream().collect(Collectors.toMap(Encoder::getMessageType, encoder -> encoder));

    private final ByteBuffer buffer = ByteBuffer.allocate(Integer.valueOf(System.getProperty("pg.io.buffer.length", "4096")));

    @Override
    @SuppressWarnings("unchecked")
    protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) {
        Encoder<Message> encoder = (Encoder<Message>) ENCODERS.get(msg.getClass());

        buffer.clear();
        ByteBuffer msgbuf = buffer;
        try {
            while (true) {
                try {
                    encoder.write(msg, msgbuf);
                    break;
                } catch (BufferOverflowException overflow) {
                    // large clob/blob, resize buffer aggressively
                    msgbuf = ByteBuffer.allocate(msgbuf.capacity() * 4);
                }
            }

            msgbuf.flip();
            out.writeBytes(msgbuf);
        } catch (Throwable t) {
            // broad catch as otherwise the exception is silently dropped
            ctx.fireExceptionCaught(t);
        }
    }

}
