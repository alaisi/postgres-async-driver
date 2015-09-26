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

import com.github.pgasync.impl.message.ExtendedQuery;

import java.nio.ByteBuffer;

/**
 * See <a href="www.postgresql.org/docs/9.3/static/protocol-message-formats.html">PostgreSQL message formats</a>
 *
 * <pre>
 * Describe (F)
 *  Byte1('D')
 *      Identifies the message as a Describe command.
 *  Int32
 *      Length of message contents in bytes, including self.
 *  Byte1
 *      'S' to describe a prepared statement; or 'P' to describe a portal.
 *  String
 *      The name of the prepared statement or portal to describe (an empty string selects the unnamed prepared statement or portal).
 *
 * Execute (F)
 *  Byte1('E')
 *      Identifies the message as an Execute command.
 *  Int32
 *      Length of message contents in bytes, including self.
 *  String
 *      The name of the portal to execute (an empty string selects the unnamed portal).
 *  Int32
 *      Maximum number of rows to return, if portal contains a queryRows that returns rows (ignored otherwise). Zero denotes "no limit".
 *
 * Sync (F)
 *  Byte1('S')
 *      Identifies the message as a Sync command.
 *  Int32(4)
 *      Length of message contents in bytes, including self.
 * </pre>
 *
 * @author Antti Laisi
 */
public class ExtendedQueryEncoder implements Encoder<ExtendedQuery> {

    @Override
    public Class<ExtendedQuery> getMessageType() {
        return ExtendedQuery.class;
    }

    @Override
    public void write(ExtendedQuery msg, ByteBuffer buffer) {
        switch (msg) {
        case DESCRIBE:
            describe(buffer);
            break;
        case EXECUTE:
            execute(buffer);
            break;
        case CLOSE:
            close(buffer);
            break;
        case SYNC:
            sync(buffer);
            break;
        default:
            throw new IllegalStateException(msg.name());
        }
    }

    void describe(ByteBuffer buffer) {
        buffer.put((byte) 'D');
        buffer.putInt(0);
        buffer.put((byte) 'S');
        buffer.put((byte) 0);
        buffer.putInt(1, buffer.position() - 1);
    }

    void execute(ByteBuffer buffer) {
        buffer.put((byte) 'E');
        buffer.putInt(0);
        buffer.put((byte) 0);
        buffer.putInt(0);
        buffer.putInt(1, buffer.position() - 1);
    }

    void close(ByteBuffer buffer) {
        buffer.put((byte) 'C');
        buffer.putInt(0);
        buffer.put((byte) 'S');
        buffer.put((byte) 0);
        buffer.putInt(1, buffer.position() - 1);
    }

    void sync(ByteBuffer buffer) {
        buffer.put((byte) 'S');
        buffer.putInt(4);
    }

}
