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
import com.github.pgasync.impl.message.Message;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Encodes PostgreSQL protocol V3 messages to bytes.
 * 
 * @author Antti Laisi
 */
public class ByteBufMessageEncoder extends MessageToByteEncoder<Message> {

    static final Map<Class<?>,Encoder<?>> ENCODERS = new HashMap<>();
    static {
        for (Encoder<?> encoder : new Encoder<?>[] {
                new SSLHandshakeEncoder(),
                new StartupMessageEncoder(),
                new PasswordMessageEncoder(),
                new QueryEncoder(), 
                new ParseEncoder(), 
                new BindEncoder(), 
                new ExtendedQueryEncoder(),
                new TerminateEncoder() }) {
            ENCODERS.put(encoder.getMessageType(), encoder);
        }
    }

    final ByteBuffer buffer = ByteBuffer.allocate(4096); // TODO: Get the value from a system property

    @Override
    @SuppressWarnings("unchecked")
    protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) throws Exception {
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
