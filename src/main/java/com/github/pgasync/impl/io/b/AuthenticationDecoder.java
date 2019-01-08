/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.pgasync.impl.io.b;

import com.github.pgasync.impl.io.Decoder;
import com.github.pgasync.impl.message.b.Authentication;

import java.nio.ByteBuffer;

/**
 * See <a href="www.postgresql.org/docs/9.3/static/protocol-message-formats.html">Postgres message formats</a>
 * 
 * <pre>
 * AuthenticationOk (B)
 *  Byte1('R')
 *      Identifies the message as an authentication request.
 *  Int32(8)
 *      Length of message contents in bytes, including self.
 *  Int32(0)
 *      Specifies that the authentication was successful.
 *       
 * AuthenticationMD5Password (B)
 *  Byte1('R')
 *      Identifies the message as an authentication request.
 *  Int32(12)
 *      Length of message contents in bytes, including self.
 *  Int32(5)
 *      Specifies that an MD5-encrypted password is required.
 *  Byte4
 *      The salt to use when encrypting the password.
 * </pre>
 * 
 * @author Antti Laisi
 */
public class AuthenticationDecoder implements Decoder<Authentication> {

    static final int OK = 0;
    static final int PASSWORD_MD5_CHALLENGE = 5;
    static final int CLEARTEXT_PASSWORD = 3;

    @Override
    public byte getMessageId() {
        return 'R';
    }

    @Override
    public Authentication read(ByteBuffer buffer) {
        int type = buffer.getInt();
        switch (type) {
            case OK:
                return new Authentication(true, null);
            case CLEARTEXT_PASSWORD:
                return new Authentication(false, null);
            case PASSWORD_MD5_CHALLENGE:
                byte[] salt = new byte[4];
                buffer.get(salt);
                return new Authentication(false, salt);
            default:
                throw new UnsupportedOperationException("Unsupported authentication type: " + type);
        }
    }

}
