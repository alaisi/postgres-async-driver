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

package com.github.pgasync.impl.io;

import com.github.pgasync.impl.message.PasswordMessage;

import java.nio.ByteBuffer;

import static com.github.pgasync.impl.io.IO.bytes;

/**
 * See <a href="www.postgresql.org/docs/9.3/static/protocol-message-formats.html">PostgreSQL message formats</a>
 *
 * <pre>
 * PasswordMessage (F)
 *  Byte1('p')
 *      Identifies the message as a password response. Note that this is also used for GSSAPI and SSPI response messages (which is really a design error, since the contained data is not a null-terminated string in that case, but can be arbitrary binary data).
 *  Int32
 *      Length of message contents in bytes, including self.
 *  String
 *      The password (encrypted, if requested).
 * </pre>
 *
 * @author Antti Laisi
 */
public class PasswordMessageEncoder implements Encoder<PasswordMessage> {

    @Override
    public Class<PasswordMessage> getMessageType() {
        return PasswordMessage.class;
    }

    @Override
    public void write(PasswordMessage msg, ByteBuffer buffer) {
        buffer.put((byte) 'p');
        buffer.putInt(0);
        buffer.put(msg.getPasswordHash() != null ? msg.getPasswordHash() : bytes(msg.getPassword()));
        buffer.put((byte) 0);
        buffer.putInt(1, buffer.position() - 1);
    }

}
