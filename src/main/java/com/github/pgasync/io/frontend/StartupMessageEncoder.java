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

import com.github.pgasync.io.Encoder;
import com.github.pgasync.message.frontend.StartupMessage;
import com.github.pgasync.io.IO;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * See <a href="www.postgresql.org/docs/9.3/static/protocol-message-formats.html">Postgres message formats</a>
 *
 * <pre>
 * StartupMessage (F)
 *  Int32
 *      Length of message contents in bytes, including self.
 *  Int32(196608)
 *      The protocol version number. The most significant 16 bits are the major version number (3 for the protocol described here). The least significant 16 bits are the minor version number (0 for the protocol described here).
 *  The protocol version number is followed by one or more pairs of parameter name and value strings. A zero byte is required as a terminator after the last name/value pair. Parameters can appear in any order. user is required, others are optional. Each parameter is specified as:
 *  String
 *      The parameter name. Currently recognized names are:
 *      user
 *          The database user name to connect as. Required; there is no default.
 *      database
 *          The database to connect to. Defaults to the user name.
 *      options
 *          Command-line arguments for the backend. (This is deprecated in favor of setting individual run-time parameters.)
 *      In addition to the above, any run-time parameter that can be set at backend start time might be listed. Such settings will be applied during backend start (after parsing the command-line options if any). The values will act as session defaults.
 *  String
 *      The parameter value.
 * </pre>
 *
 * @author Antti Laisi
 */
public class StartupMessageEncoder implements Encoder<StartupMessage> {

    @Override
    public Class<StartupMessage> getMessageType() {
        return StartupMessage.class;
    }

    @Override
    public void write(StartupMessage msg, ByteBuffer buffer, Charset encoding) {
        buffer.putInt(0);
        buffer.putInt(msg.getProtocol());

        for (String s : new String[]{
                "user", msg.getUsername(),
                "database", msg.getDatabase(),
                "client_encoding", encoding.name()
        }) {
            IO.putCString(buffer, s, StandardCharsets.US_ASCII);
        }

        buffer.put((byte) 0);
        buffer.putInt(0, buffer.position());
    }

}
