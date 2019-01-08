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
import com.github.pgasync.impl.io.b.AuthenticationDecoder;
import com.github.pgasync.impl.io.b.BindCompleteDecoder;
import com.github.pgasync.impl.io.b.CloseCompleteDecoder;
import com.github.pgasync.impl.io.b.CommandCompleteDecoder;
import com.github.pgasync.impl.io.b.DataRowDecoder;
import com.github.pgasync.impl.io.b.ErrorResponseDecoder;
import com.github.pgasync.impl.io.b.NoticeResponseDecoder;
import com.github.pgasync.impl.io.b.NotificationResponseDecoder;
import com.github.pgasync.impl.io.b.ParseCompleteDecoder;
import com.github.pgasync.impl.io.b.ReadyForQueryDecoder;
import com.github.pgasync.impl.io.b.RowDescriptionDecoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Decodes incoming bytes to Postgres V3 protocol message instances.
 *
 * @author Antti Laisi
 */
class ByteBufMessageDecoder extends ByteToMessageDecoder {

    static final Map<Byte, Decoder<?>> DECODERS = Set.of(new ErrorResponseDecoder(),
            new AuthenticationDecoder(),
            new ReadyForQueryDecoder(),
            new RowDescriptionDecoder(),
            new ParseCompleteDecoder(),
            new CloseCompleteDecoder(),
            new BindCompleteDecoder(),
            new CommandCompleteDecoder(),
            new DataRowDecoder(),
            new NotificationResponseDecoder(),
            new NoticeResponseDecoder()
    ).stream().collect(
            Collectors.toMap(Decoder::getMessageId, Function.identity())
    );

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() > 0) {
            byte id = in.readByte();
            int length = in.readInt();

            Decoder<?> decoder = DECODERS.get(id);
            try {
                if (decoder != null) {
                    ByteBuffer buffer = in.nioBuffer();
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
}
