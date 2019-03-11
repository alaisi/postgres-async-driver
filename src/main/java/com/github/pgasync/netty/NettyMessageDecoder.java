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

import com.github.pgasync.io.Decoder;
import com.github.pgasync.io.backend.AuthenticationDecoder;
import com.github.pgasync.io.backend.BindCompleteDecoder;
import com.github.pgasync.io.backend.CloseCompleteDecoder;
import com.github.pgasync.io.backend.CommandCompleteDecoder;
import com.github.pgasync.io.backend.DataRowDecoder;
import com.github.pgasync.io.backend.ErrorResponseDecoder;
import com.github.pgasync.io.backend.NoDataDecoder;
import com.github.pgasync.io.backend.NoticeResponseDecoder;
import com.github.pgasync.io.backend.NotificationResponseDecoder;
import com.github.pgasync.io.backend.ParseCompleteDecoder;
import com.github.pgasync.io.backend.ReadyForQueryDecoder;
import com.github.pgasync.io.backend.RowDescriptionDecoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Decodes incoming bytes to Postgres V11 protocol message instances.
 *
 * @author Antti Laisi
 */
class NettyMessageDecoder extends ByteToMessageDecoder {

    private static final Map<Byte, Decoder<?>> DECODERS = Set.of(
            new ErrorResponseDecoder(),
            new AuthenticationDecoder(),
            new ReadyForQueryDecoder(),
            new RowDescriptionDecoder(),
            new ParseCompleteDecoder(),
            new CloseCompleteDecoder(),
            new BindCompleteDecoder(),
            new NoDataDecoder(),
            new CommandCompleteDecoder(),
            new DataRowDecoder(),
            new NotificationResponseDecoder(),
            new NoticeResponseDecoder()
    ).stream().collect(
            Collectors.toMap(Decoder::getMessageId, Function.identity())
    );

    private final Charset encoding;

    public NettyMessageDecoder(Charset encoding) {
        this.encoding = encoding;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() > 0) {
            byte id = in.readByte();
            int length = in.readInt();

            Decoder<?> decoder = DECODERS.get(id);
            try {
                if (decoder != null) {
                    ByteBuffer buffer = in.nioBuffer();
                    out.add(decoder.read(buffer, encoding));
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
