package com.github.pgasync.impl.io.frontend;

import com.github.pgasync.impl.io.Encoder;
import com.github.pgasync.impl.message.Message;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * @param <M> specific {@link Message} type.
 * @author Marat Gainullin
 */
public abstract class SkipableEncoder<M extends Message> implements Encoder<M> {

    @Override
    public void write(M msg, ByteBuffer buffer, Charset encoding) {
        buffer.put(getMessageId());
        buffer.putInt(0);
        writeBody(msg, buffer, encoding);
        buffer.putInt(1, buffer.position() - 1);
    }

    protected abstract byte getMessageId();

    protected abstract void writeBody(M msg, ByteBuffer buffer, Charset encoding);

}
