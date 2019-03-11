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

package com.github.pgasync.message.backend;

import com.github.pgasync.message.Message;

/**
 * @author Antti Laisi
 */
public class LogResponse implements Message {

    protected final String level;
    protected final String code;
    protected final String message;

    public LogResponse(String level, String code, String message) {
        this.level = level;
        this.code = code;
        this.message = message;
    }

    public String getLevel() {
        return level;
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

}
