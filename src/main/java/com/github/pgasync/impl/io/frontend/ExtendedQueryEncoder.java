package com.github.pgasync.impl.io.frontend;

import com.github.pgasync.impl.message.Message;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public abstract class ExtendedQueryEncoder<M extends Message> extends SkipableEncoder<M> {

    @Override
    public void write(M msg, ByteBuffer buffer, Charset encoding) {
        super.write(msg, buffer, encoding);
        // Flush
        buffer.put((byte) 'H');
        buffer.putInt(4);
    }
}
