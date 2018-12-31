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

/**
 * @author  Antti Laisi
 */
public class ErrorResponse implements Message {

    public enum Level {
        ERROR, FATAL, PANIC, WARNING, NOTICE, DEBUG, INFO, LOG
    }

    final Level level;
    final String code;
    final String message;

    public ErrorResponse(String levelName, String code, String message) {
        this.level = Level.valueOf(levelName);
        this.code = code;
        this.message = message;
    }

    public Level getLevel() {
        return level;
    }
    public String getCode() {
        return code;
    }
    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return String.format("ErrorResponse(level=%s,code=%s,message=%s)", level, code, message);
    }
}
