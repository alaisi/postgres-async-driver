package com.github.pgasync.impl.io.frontend;

import com.github.pgasync.impl.message.Message;

import java.nio.ByteBuffer;

public abstract class ExtendedQueryEncoder<M extends Message> extends SkipableEncoder<M> {

    @Override
    public void write(M msg, ByteBuffer buffer) {
        super.write(msg, buffer);
        // Flush
        buffer.put((byte) 'H');
        buffer.putInt(4);
    }
}
