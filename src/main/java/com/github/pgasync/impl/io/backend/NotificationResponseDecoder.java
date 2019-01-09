package com.github.pgasync.impl.io.backend;

import com.github.pgasync.impl.io.Decoder;
import com.github.pgasync.impl.message.backend.NotificationResponse;

import java.nio.ByteBuffer;

import static com.github.pgasync.impl.io.IO.getCString;

/**
 * See <a href="https://www.postgresql.org/docs/11/protocol-message-formats.html">Postgres message formats</a>
 *
 * <pre>
 * NotificationResponse (B)
 *  Byte1('A')
 *      Identifies the message as a notification response.
 *  Int32
 *      Length of message contents in bytes, including self.
 *  Int32
 *      The process ID of the notifying backend process.
 *  String
 *      The name of the channel that the notify has been raised on.
 *  String
 *      The "payload" string passed from the notifying process.
 * </pre>
 *
 * @author Antti Laisi
 */
public class NotificationResponseDecoder implements Decoder<NotificationResponse> {

    @Override
    public NotificationResponse read(ByteBuffer buffer) {
        return new NotificationResponse(buffer.getInt(), getCString(buffer), getCString(buffer));
    }

    @Override
    public byte getMessageId() {
        return 'A';
    }
}
