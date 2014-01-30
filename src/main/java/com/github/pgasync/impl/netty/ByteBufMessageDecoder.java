package com.github.pgasync.impl.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.pgasync.impl.io.AuthenticationDecoder;
import com.github.pgasync.impl.io.CommandCompleteDecoder;
import com.github.pgasync.impl.io.DataRowDecoder;
import com.github.pgasync.impl.io.Decoder;
import com.github.pgasync.impl.io.ErrorResponseDecoder;
import com.github.pgasync.impl.io.ReadyForQueryDecoder;
import com.github.pgasync.impl.io.RowDescriptionDecoder;

class ByteBufMessageDecoder extends ByteToMessageDecoder {

	static final Map<Byte, Decoder<?>> DECODERS = new HashMap<>();
	static {
		for(Decoder<?> decoder : new Decoder<?>[]{ 
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
		if(in.readableBytes() == 0) {
			return;
		}

		byte id = in.readByte();
		int length = in.readInt();

		ByteBuffer buffer = in.nioBuffer();
		Decoder<?> decoder =  DECODERS.get(id);

		try {
			if(decoder != null) {
				out.add(decoder.read(buffer));
				in.skipBytes(buffer.position());
			} else {
				in.skipBytes(length - 4);
			}
		} catch(Throwable t) {
			// broad catch as otherwise the exception is silently dropped
			ctx.fireExceptionCaught(t);
		}
	}
}
