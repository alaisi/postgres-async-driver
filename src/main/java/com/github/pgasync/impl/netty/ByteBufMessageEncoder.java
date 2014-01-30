package com.github.pgasync.impl.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.github.pgasync.impl.io.BindEncoder;
import com.github.pgasync.impl.io.Encoder;
import com.github.pgasync.impl.io.ExtendedQueryEncoder;
import com.github.pgasync.impl.io.ParseEncoder;
import com.github.pgasync.impl.io.PasswordMessageEncoder;
import com.github.pgasync.impl.io.QueryEncoder;
import com.github.pgasync.impl.io.StartupEncoder;
import com.github.pgasync.impl.message.Message;

public class ByteBufMessageEncoder extends MessageToByteEncoder<Message> {

	static final Map<Class<?>, Encoder<?>> ENCODERS = new HashMap<>();
	static {
		for(Encoder<?> encoder : new Encoder<?>[]{
				new StartupEncoder(),
				new PasswordMessageEncoder(),
				new QueryEncoder(),
				new ParseEncoder(),
				new BindEncoder(),
				new ExtendedQueryEncoder() }) {
			ENCODERS.put(encoder.getMessageType(), encoder);
		}
	}

	final ByteBuffer buffer = ByteBuffer.allocate(4096);

	@Override
	@SuppressWarnings("unchecked")
	protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) throws Exception {
		Encoder<Message> encoder = (Encoder<Message>) ENCODERS.get(msg.getClass());

		buffer.clear();
		ByteBuffer msgbuf = buffer;
		while(true) {
			try {
				encoder.write(msg, msgbuf);
				break;
			} catch(BufferOverflowException overflow) {
				// large clob/blob, resize buffer aggressively
				msgbuf = ByteBuffer.allocate(msgbuf.capacity() * 4);
			} catch(Throwable t) {
				// broad catch as otherwise the exception is silently dropped
				ctx.fireExceptionCaught(t);
				break;
			}
		}

		msgbuf.flip();
		out.writeBytes(msgbuf);
	}

}
