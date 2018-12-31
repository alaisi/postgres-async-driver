package com.github.pgasync.impl.io;

import com.github.pgasync.impl.message.SSLRequest;

import java.nio.ByteBuffer;

/**
 * @author Antti Laisi
 */
public class SSLHandshakeEncoder implements Encoder<SSLRequest> {

    @Override
    public Class<SSLRequest> getMessageType() {
        return SSLRequest.class;
    }

    @Override
    public void write(SSLRequest msg, ByteBuffer buffer) {
        buffer.putInt(8);
        buffer.putInt(80877103);
    }
}
