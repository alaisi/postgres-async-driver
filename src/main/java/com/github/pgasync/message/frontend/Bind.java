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

package com.github.pgasync.message.frontend;

import com.github.pgasync.message.ExtendedQueryMessage;

/**
 * @author Antti Laisi
 */
public class Bind implements ExtendedQueryMessage {

    private final String sname;
    private final byte[][] params;

    public Bind(String sname, byte[][] params) {
        this.sname = sname;
        this.params = params;
    }

    public String getSname() {
        return sname;
    }

    public byte[][] getParams() {
        return params;
    }
}
