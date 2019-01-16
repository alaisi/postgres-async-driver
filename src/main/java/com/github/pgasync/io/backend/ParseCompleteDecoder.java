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

package com.github.pgasync.io.backend;

import com.github.pgasync.io.Decoder;
import com.github.pgasync.message.backend.BIndicators;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * See <a https://www.postgresql.org/docs/11/protocol-message-formats.html">Postgres message formats</a>
 *
 * <pre>
 * ParseComplete (B)
 *  Byte1('1')
 *      Identifies the message as a Parse-complete indicator.
 *
 *  Int32(4)
 *      Length of message contents in bytes, including self.
 *
 * </pre>
 *
 * @author Marat Gainullin
 */
public class ParseCompleteDecoder implements Decoder<BIndicators> {

    @Override
    public byte getMessageId() {
        return '1';
    }

    @Override
    public BIndicators read(ByteBuffer buffer, Charset encoding) {
        return BIndicators.PARSE_COMPLETE;
    }

}
