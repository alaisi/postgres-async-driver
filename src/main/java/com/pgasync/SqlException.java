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

package com.pgasync;

import java.util.function.Consumer;

/**
 * Backend or client error. If the error is sent by backend, SQLSTATE error code
 * is available.
 *
 * @author Antti Laisi
 */
public class SqlException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    private static final int MAX_CAUSE_DEPTH = 100;

    final String code;

    public SqlException(String level, String code, String message) {
        super(level + ": SQLSTATE=" + code + ", MESSAGE=" + message);
        this.code = code;
    }

    public SqlException(String message) {
        super(message);
        this.code = null;
    }

    public SqlException(Throwable cause) {
        super(cause);
        this.code = null;
    }

    public String getCode() {
        return code;
    }

    @FunctionalInterface
    public interface CheckedRunnable {

        void run() throws Exception;
    }

    public static boolean ifCause(Throwable th, Consumer<SqlException> action, CheckedRunnable others) throws Exception {
        int depth = 1;
        while (depth++ < MAX_CAUSE_DEPTH && th != null && !(th instanceof SqlException)) {
            th = th.getCause();
        }
        if (th instanceof SqlException) {
            action.accept((SqlException) th);
            return true;
        } else {
            others.run();
            return false;
        }
    }
}
