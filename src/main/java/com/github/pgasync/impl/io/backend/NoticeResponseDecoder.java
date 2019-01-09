package com.github.pgasync.impl.io.backend;

import com.github.pgasync.impl.io.Decoder;
import com.github.pgasync.impl.message.backend.NoticeResponse;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.github.pgasync.impl.io.IO.getCString;

/**
 * See <a href="https://www.postgresql.org/docs/11/protocol-message-formats.html">Postgres message formats</a>
 *
 * <pre>
 * NoticeResponse (B)
 * Byte1('N')
 *     Identifies the message as a notice.
 *
 * Int32
 *     Length of message contents in bytes, including self.
 *
 * The message body consists of one or more identified fields, followed by a zero byte as a terminator. Fields can appear in any order. For each field there is the following:
 *
 * Byte1
 *     A code identifying the field type; if zero, this is the message terminator and no string follows. The presently defined field types are listed in Section 53.8. Since more field types might be added in future, frontends should silently ignore fields of unrecognized type.
 *
 * String
 *     The field value.
 * </pre>
 *
 * @author Marat Gainullin
 */
public class NoticeResponseDecoder implements Decoder<NoticeResponse> {

    @Override
    public NoticeResponse read(ByteBuffer buffer) {
        List<NoticeResponse.Field> fields = new ArrayList<>();
        for (byte fieldType = buffer.get(); fieldType != 0; fieldType = buffer.get()) {
            fields.add(NoticeResponse.Field.of(fieldType, getCString(buffer)));
        }
        return new NoticeResponse(fields.toArray(new NoticeResponse.Field[]{}));
    }

    @Override
    public byte getMessageId() {
        return 'N';
    }
}
