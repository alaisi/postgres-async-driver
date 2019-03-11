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

import com.github.pgasync.message.frontend.Query;
import com.github.pgasync.io.IO;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * See <a href="https://www.postgresql.org/docs/11/protocol-message-formats.html">Postgres message formats</a>
 *
 * <pre>
 * Query (F)
 *  Byte1('Q')
 *      Identifies the message as a simple query.
 *  Int32
 *      Length of message contents in bytes, including self.
 *  String
 *      The query string itself.
 * </pre>
 *
 * @author Antti Laisi
 */
public class QueryEncoder extends SkipableEncoder<Query> {

    @Override
    public Class<Query> getMessageType() {
        return Query.class;
    }

    @Override
    protected byte getMessageId() {
        return (byte) 'Q';
    }

    @Override
    public void writeBody(Query msg, ByteBuffer buffer, Charset encoding) {
        IO.putCString(buffer, msg.getQuery(), encoding);
    }
}
