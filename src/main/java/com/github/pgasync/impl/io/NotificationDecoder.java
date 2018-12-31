package com.github.pgasync.impl.io;

import com.github.pgasync.impl.message.Notification;

import java.nio.ByteBuffer;

import static com.github.pgasync.impl.io.IO.getCString;

/**
 * See <a href="www.postgresql.org/docs/9.3/static/protocol-message-formats.html">PostgreSQL message formats</a>
 *
 * <pre>
 * Notification (B)
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
public class NotificationDecoder implements Decoder<Notification> {

    @Override
    public Notification read(ByteBuffer buffer) {
        byte[] chars = new byte[255];
        return new Notification(buffer.getInt(), getCString(buffer, chars), getCString(buffer, chars));
    }

    @Override
    public byte getMessageId() {
        return 'A';
    }
}
