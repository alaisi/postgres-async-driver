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
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Decodes incoming bytes to PostgreSQL V3 protocol message instances.
 * 
 * @author Antti Laisi
 */
class ByteBufMessageDecoder extends ByteToMessageDecoder {

    static final Map<Byte,Decoder<?>> DECODERS = new HashMap<>();
    static {
        for (Decoder<?> decoder : new Decoder<?>[] { 
                new ErrorResponseDecoder(), 
                new AuthenticationDecoder(),
                new ReadyForQueryDecoder(), 
                new RowDescriptionDecoder(), 
                new CommandCompleteDecoder(),
                new DataRowDecoder() }) {
            DECODERS.put(decoder.getMessageId(), decoder);
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() == 0) {
            return;
        }

        byte id = in.readByte();
        int length = in.readInt();

        ByteBuffer buffer = in.nioBuffer();
        Decoder<?> decoder = DECODERS.get(id);

        try {
            if (decoder != null) {
                out.add(decoder.read(buffer));
                in.skipBytes(buffer.position());
            } else {
                in.skipBytes(length - 4);
            }
        } catch (Throwable t) {
            // broad catch as otherwise the exception is silently dropped
            ctx.fireExceptionCaught(t);
        }
    }
}
