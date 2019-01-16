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
import com.github.pgasync.message.backend.LogResponse;
import com.github.pgasync.io.IO;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Base class for NoticeResponse and for ErrorResponse.
 *
 * @param <M> Specific message type.
 * @author Marat Gainullin
 */
public abstract class LogResponseDecoder<M extends LogResponse> implements Decoder<M> {

    @Override
    public M read(ByteBuffer buffer, Charset encoding) {
        String level = null;
        String code = null;
        String message = null;
        for (byte type = buffer.get(); type != 0; type = buffer.get()) {
            String value = IO.getCString(buffer, encoding);
            if (type == (byte) 'S') {
                level = value;
            } else if (type == (byte) 'C') {
                code = value;
            } else if (type == (byte) 'M') {
                message = value;
            }
        }
        return asMessage(level, code, message);
    }

    protected abstract M asMessage(String level, String code, String message);

}
