package com.github.pgasync.impl.io.frontend;

import com.github.pgasync.impl.io.Encoder;
import com.github.pgasync.impl.message.frontend.SSLRequest;

import java.nio.ByteBuffer;

/**
 * @author Marat Gainullin
 */
public class SSLRequestEncoder implements Encoder<SSLRequest> {

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
