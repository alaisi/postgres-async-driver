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
 * @author Marat Gainullin
 */
public class Close implements ExtendedQueryMessage {

    public enum Kind {
        STATEMENT((byte) 'S'),
        PORTAL((byte) 'P');

        byte code;

        Kind(byte code) {
            this.code = code;
        }

        public byte getCode() {
            return code;
        }
    }

    private final Kind kind;
    private final String name;

    private Close(String name, Kind kind) {
        this.name = name;
        this.kind = kind;
    }

    public String getName() {
        return name;
    }

    public Kind getKind() {
        return kind;
    }

    public static Close portal(String name) {
        return new Close(name, Kind.PORTAL);
    }

    public static Close statement(String name) {
        return new Close(name, Kind.STATEMENT);
    }
}
