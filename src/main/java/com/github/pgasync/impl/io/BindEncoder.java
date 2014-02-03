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

import java.nio.ByteBuffer;

import com.github.pgasync.impl.message.Bind;

public class BindEncoder implements Encoder<Bind> {

    @Override
    public Class<Bind> getMessageType() {
        return Bind.class;
    }

    @Override
    public void write(Bind msg, ByteBuffer buffer) {
        buffer.put((byte) 'B');
        buffer.putInt(0);
        buffer.put((byte) 0); // portal
        buffer.put((byte) 0); // prepared statement
        buffer.putShort((short) 0); // number of format codes
        buffer.putShort((short) msg.getParams().length); // number of parameters
        for (byte[] param : msg.getParams()) {
            writeParameter(buffer, param);
        }
        buffer.putShort((short) 0);
        buffer.putInt(1, buffer.position() - 1);
    }

    void writeParameter(ByteBuffer buffer, byte[] param) {
        if (param == null) {
            buffer.putInt(-1);
            return;
        }
        buffer.putInt(param.length);
        buffer.put(param);
    }
}
