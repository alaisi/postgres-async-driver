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

import com.github.pgasync.impl.message.Parse;

import java.nio.ByteBuffer;

import static com.github.pgasync.impl.io.IO.putCString;

/**
 * See <a href="www.postgresql.org/docs/9.3/static/protocol-message-formats.html">PostgreSQL message formats</a>
 *
 * <pre>
 * Parse (F)
 *  Byte1('P')
 *      Identifies the message as a Parse command.
 *  Int32
 *      Length of message contents in bytes, including self.
 *  String
 *      The name of the destination prepared statement (an empty string selects the unnamed prepared statement).
 *  String
 *      The query string to be parsed.
 *  Int16
 *      The number of parameter data types specified (can be zero). Note that this is not an indication of the number of parameters that might appear in the query string, only the number that the frontend wants to prespecify types for.
 *  Then, for each parameter, there is the following:
 *  Int32
 *      Specifies the object ID of the parameter data type. Placing a zero here is equivalent to leaving the type unspecified.
 * </pre>
 *
 * @author Antti Laisi
 */
public class ParseEncoder implements Encoder<Parse> {

    @Override
    public Class<Parse> getMessageType() {
        return Parse.class;
    }

    @Override
    public void write(Parse msg, ByteBuffer buffer) {
        buffer.put((byte) 'P');
        buffer.putInt(0);
        buffer.put((byte) 0); // unnamed prepared statement
        putCString(buffer, msg.getQuery());
        buffer.putShort((short) 0); // no parameter types
        buffer.putInt(1, buffer.position() - 1);
    }

}
