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

import static com.github.pgasync.impl.io.IO.putCString;

import java.nio.ByteBuffer;

import com.github.pgasync.impl.message.Parse;

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
