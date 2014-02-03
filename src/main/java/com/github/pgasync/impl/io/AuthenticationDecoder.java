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

import com.github.pgasync.impl.message.Authentication;

public class AuthenticationDecoder implements Decoder<Authentication> {

    static final int OK = 0;
    static final int PASSWORD_MD5_CHALLENGE = 5;

    @Override
    public byte getMessageId() {
        return (byte) 'R';
    }

    @Override
    public Authentication read(ByteBuffer buffer) {
        Authentication msg = new Authentication();
        int type = buffer.getInt();
        if (type == OK) {
            msg.setAuthenticationOk();
        } else if (type == PASSWORD_MD5_CHALLENGE) {
            msg.setMd5Salt(new byte[4]);
            buffer.get(msg.getMd5Salt());
        }
        return msg;
    }

}
