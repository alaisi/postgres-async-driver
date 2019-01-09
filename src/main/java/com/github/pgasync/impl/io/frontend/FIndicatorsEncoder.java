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

package com.github.pgasync.impl.io.frontend;

import com.github.pgasync.impl.io.Encoder;
import com.github.pgasync.impl.message.frontend.FIndicators;

import java.nio.ByteBuffer;

/**
 * See <a href="https://www.postgresql.org/docs/11/protocol-message-formats.html">Postgres message formats</a>
 *
 * <pre>
 * Sync (F)
 *  Byte1('S')
 *      Identifies the message as a Sync command.
 *  Int32(4)
 *      Length of message contents in bytes, including self.
 * </pre>
 *
 * @author Antti Laisi
 */
public class FIndicatorsEncoder implements Encoder<FIndicators> {

    @Override
    public Class<FIndicators> getMessageType() {
        return FIndicators.class;
    }

    @Override
    public void write(FIndicators msg, ByteBuffer buffer) {
        switch (msg) {
            case SYNC:
                sync(buffer);
                break;
            default:
                throw new IllegalStateException(msg.name());
        }
    }

    void sync(ByteBuffer buffer) {
        buffer.put((byte) 'S');
        buffer.putInt(4);
    }

}
