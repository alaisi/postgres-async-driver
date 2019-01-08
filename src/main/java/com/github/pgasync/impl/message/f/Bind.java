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

package com.github.pgasync.impl.message.f;

import com.github.pgasync.impl.message.Message;

/**
 * @author Antti Laisi
 */
public class Bind implements Message {

    final String sname;
    final String pname;
    final byte[][] params;

    public Bind(String sname, String pname, byte[][] params) {
        this.sname = sname;
        this.pname = pname;
        this.params = params;
    }

    public String getSname() {
        return sname;
    }

    public String getPname() {
        return pname;
    }

    public byte[][] getParams() {
        return params;
    }
}
