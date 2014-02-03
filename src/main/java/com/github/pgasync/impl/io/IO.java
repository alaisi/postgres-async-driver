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
import java.nio.charset.Charset;

/**
 * Static utility methods for input/output.
 * 
 * @author Antti Laisi
 */
public enum IO {
    ;

    public static final Charset UTF8 = Charset.forName("UTF-8");

    public static byte[] bytes(String s) {
        return s.getBytes(UTF8);
    }

    public static String getCString(ByteBuffer buffer, byte[] store) {
        int i = 0;
        for (byte c = buffer.get(); c != 0; c = buffer.get()) {
            store[i++] = c;
        }
        return new String(store, 0, i, UTF8);
    }

    public static void putCString(ByteBuffer buffer, String string) {
        buffer.put(bytes(string));
        buffer.put((byte) 0);
    }

}
