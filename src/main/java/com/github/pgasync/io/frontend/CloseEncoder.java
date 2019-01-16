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

package com.github.pgasync.io.frontend;

import com.github.pgasync.io.IO;
import com.github.pgasync.message.frontend.Close;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * See <https://www.postgresql.org/docs/11/protocol-message-formats.html">Postgres message formats</a>
 *
 * <pre>
 * Close (F)
 *   Byte1('C')
 *   Identifies the message as a Close command.
 *
 *   Int32
 *   Length of message contents in bytes, including self.
 *
 *   Byte1
 *   'S' to close a prepared statement; or 'P' to close a portal.
 *
 *   String
 *   The name of the prepared statement or portal to close (an empty string selects the unnamed prepared statement or portal).
 *
 * </pre>
 *
 * @author Marat Gainullin
 */
public class CloseEncoder extends ExtendedQueryEncoder<Close> {

    @Override
    public Class<Close> getMessageType() {
        return Close.class;
    }

    @Override
    protected byte getMessageId() {
        return (byte) 'C';
    }

    @Override
    public void writeBody(Close msg, ByteBuffer buffer, Charset encoding) {
        buffer.put(msg.getKind().getCode());
        IO.putCString(buffer, msg.getName(), encoding);
    }
}
