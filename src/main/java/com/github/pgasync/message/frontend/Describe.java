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
public class Describe implements ExtendedQueryMessage {

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

    Describe(String name, Kind kind) {
        this.name = name;
        this.kind = kind;
    }

    public Kind getKind() {
        return kind;
    }

    public String getName() {
        return name;
    }

    public static Describe portal() {
        return new Describe("", Kind.PORTAL);
    }

    public static Describe statement(String name) {
        return new Describe(name, Kind.STATEMENT);
    }
}
