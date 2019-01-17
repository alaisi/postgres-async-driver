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
import com.github.pgasync.message.frontend.Execute;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * See <a href="https://www.postgresql.org/docs/11/protocol-message-formats.html">Postgres message formats</a>
 *
 * <pre>
 * Execute (F)
 *   Byte1('E')
 *       Identifies the message as an Execute command.
 *
 *   Int32
 *       Length of message contents in bytes, including self.
 *
 *   String
 *       The name of the portal to execute (an empty string selects the unnamed portal).
 *
 *   Int32
 *       Maximum number of rows to return, if portal contains a query that returns rows (ignored otherwise). Zero denotes “no limit”.
 * </pre>
 *
 * @author Marat Gainullin
 */
public class ExecuteEncoder extends ExtendedQueryEncoder<Execute> {

    @Override
    public Class<Execute> getMessageType() {
        return Execute.class;
    }

    @Override
    protected byte getMessageId() {
        return (byte) 'E';
    }

    @Override
    public void writeBody(Execute msg, ByteBuffer buffer, Charset encoding) {
        IO.putCString(buffer, "", encoding); // unnamed portal
        buffer.putInt(0); // unlimited maximum rows
    }
}
