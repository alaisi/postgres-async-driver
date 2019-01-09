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

package com.github.pgasync.impl.io.backend;

import com.github.pgasync.impl.io.Decoder;
import com.github.pgasync.impl.message.backend.BIndicators;

import java.nio.ByteBuffer;

/**
 * See <a https://www.postgresql.org/docs/11/protocol-message-formats.html">Postgres message formats</a>
 *
 * <pre>
 * CloseComplete (B)
 *  Byte1('3')
 *      Identifies the message as a Close-complete indicator.
 *
 *  Int32(4)
 *      Length of message contents in bytes, including self.
 *
 * </pre>
 *
 * @author Marat Gainullin
 */
public class CloseCompleteDecoder implements Decoder<BIndicators> {

    @Override
    public byte getMessageId() {
        return '3';
    }

    @Override
    public BIndicators read(ByteBuffer buffer) {
        return BIndicators.CLOSE_COMPLETE;
    }

}
