package com.github.pgasync.io.backend;

import com.github.pgasync.message.backend.NoticeResponse;

/**
 * See <a href="https://www.postgresql.org/docs/11/protocol-message-formats.html">Postgres message formats</a>
 *
 * <pre>
 * NoticeResponse (B)
 * Byte1('N')
 *     Identifies the message as a notice.
 * Int32
 *     Length of message contents in bytes, including self.
 * The message body consists of one or more identified fields, followed by a zero byte as a terminator. Fields can appear in any order. For each field there is the following:
 * Byte1
 *     A code identifying the field type; if zero, this is the message terminator and no string follows. The presently defined field types are listed in Section 53.8. Since more field types might be added in future, frontends should silently ignore fields of unrecognized type.
 * String
 *     The field value.
 * </pre>
 *
 * @author Marat Gainullin
 */
public class NoticeResponseDecoder extends LogResponseDecoder<NoticeResponse> {

    @Override
    public byte getMessageId() {
        return 'N';
    }

    @Override
    protected NoticeResponse asMessage(String level, String code, String message) {
        return new NoticeResponse(level, code, message);
    }

}
