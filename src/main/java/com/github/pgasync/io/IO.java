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

package com.github.pgasync.io;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Static utility methods for input/output.
 *
 * @author Antti Laisi
 */
public class IO {

    public static String getCString(ByteBuffer buffer, Charset charset) {
        ByteArrayOutputStream readBuffer = new ByteArrayOutputStream(255);
        for (int c = buffer.get(); c != 0; c = buffer.get()) {
            readBuffer.write(c);
        }
        return new String(readBuffer.toByteArray(), charset);
    }

    public static void putCString(ByteBuffer buffer, String value, Charset charset) {
        if (!value.isEmpty()) {
            buffer.put(value.getBytes(charset));
        }
        buffer.put((byte) 0);
    }

}
