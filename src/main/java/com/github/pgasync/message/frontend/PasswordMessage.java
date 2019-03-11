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

import com.github.pgasync.message.Message;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static javax.xml.bind.DatatypeConverter.printHexBinary;

/**
 * @author Antti Laisi
 */
public class PasswordMessage implements Message {

    private final String password;
    private final byte[] passwordHash;

    public PasswordMessage(String username, String password, byte[] md5salt, Charset encoding) {
        this.password = password;
        this.passwordHash = md5salt != null ? md5(username, password, md5salt, encoding) : null;
    }

    public byte[] getPasswordHash() {
        return passwordHash;
    }

    public String getPassword() {
        return password;
    }

    private static byte[] md5(String username, String password, byte[] md5salt, Charset encoding) {
        MessageDigest md5 = md5();
        md5.update(password.getBytes(encoding));
        md5.update(username.getBytes(encoding));
        byte[] hash = printHexBinary(md5.digest()).toLowerCase().getBytes(encoding);

        md5.update(hash);
        md5.update(md5salt);
        hash = printHexBinary(md5.digest()).toLowerCase().getBytes(encoding);

        byte[] prefixed = new byte[hash.length + 3];
        prefixed[0] = (byte) 'm';
        prefixed[1] = (byte) 'd';
        prefixed[2] = (byte) '5';
        System.arraycopy(hash, 0, prefixed, 3, hash.length);
        return prefixed;
    }

    private static MessageDigest md5() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
        }
    }
}
