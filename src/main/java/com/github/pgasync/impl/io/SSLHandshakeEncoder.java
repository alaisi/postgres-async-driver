package com.github.pgasync.impl.io;

import com.github.pgasync.impl.message.SSLHandshake;

import java.nio.ByteBuffer;

/**
 * @author Antti Laisi
 */
public class SSLHandshakeEncoder implements Encoder<SSLHandshake> {

    @Override
    public Class<SSLHandshake> getMessageType() {
        return SSLHandshake.class;
    }

    @Override
    public void write(SSLHandshake msg, ByteBuffer buffer) {
        buffer.putInt(8);
        buffer.putInt(80877103);
    }
}
