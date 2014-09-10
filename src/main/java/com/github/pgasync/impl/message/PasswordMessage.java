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

package com.github.pgasync.impl.message;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static com.github.pgasync.impl.io.IO.bytes;
import static javax.xml.bind.DatatypeConverter.printHexBinary;

/**
 * @author  Antti Laisi
 */
public class PasswordMessage implements Message {

    final String password;
    final byte[] passwordHash;

    public PasswordMessage(String username, String password, byte[] md5salt) {
        this.password = password;
        this.passwordHash = md5salt != null ? md5(username, password, md5salt) : null;
    }

    public byte[] getPasswordHash() {
        return passwordHash;
    }

    public String getPassword() {
        return password;
    }

    static byte[] md5(String username, String password, byte[] md5salt) {
        MessageDigest md5 = digest();
        md5.update(bytes(password));
        md5.update(bytes(username));
        byte[] hash = bytes(printHexBinary(md5.digest()).toLowerCase());

        md5.update(hash);
        md5.update(md5salt);
        hash = bytes(printHexBinary(md5.digest()).toLowerCase());

        byte[] prefixed = new byte[hash.length + 3];
        prefixed[0] = (byte) 'm';
        prefixed[1] = (byte) 'd';
        prefixed[2] = (byte) '5';
        System.arraycopy(hash, 0, prefixed, 3, hash.length);
        return prefixed;
    }

    static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
