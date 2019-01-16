package com.github.pgasync.io.frontend;

import com.github.pgasync.message.ExtendedQueryMessage;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public abstract class ExtendedQueryEncoder<M extends ExtendedQueryMessage> extends SkipableEncoder<M> {

    @Override
    public void write(M msg, ByteBuffer buffer, Charset encoding) {
        super.write(msg, buffer, encoding);
        // Flush
        buffer.put((byte) 'H');
        buffer.putInt(4);
    }
}
