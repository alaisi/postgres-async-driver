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
import com.github.pgasync.message.frontend.Describe;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * See <a href="www.postgresql.org/docs/9.3/static/protocol-message-formats.html">Postgres message formats</a>
 *  
 * <pre>
 *  Describe (F)
 *   Byte1('D')
 *       Identifies the message as a Describe command.
 *
 *   Int32
 *       Length of message contents in bytes, including self.
 *
 *   Byte1
 *       'S' to describe a prepared statement; or 'P' to describe a portal.
 *
 *   String
 *       The name of the prepared statement or portal to describe (an empty string selects the unnamed prepared statement or portal).
 *
 *</pre>
 *
 * @author Marat Gainullin
 */
public class DescribeEncoder extends ExtendedQueryEncoder<Describe> {

    @Override
    public Class<Describe> getMessageType() {
        return Describe.class;
    }

    @Override
    protected byte getMessageId() {
        return (byte) 'D';
    }

    @Override
    public void writeBody(Describe msg, ByteBuffer buffer, Charset encoding) {
        // Describe
        buffer.put(msg.getKind().getCode());
        IO.putCString(buffer, msg.getName(), encoding);
    }
}
