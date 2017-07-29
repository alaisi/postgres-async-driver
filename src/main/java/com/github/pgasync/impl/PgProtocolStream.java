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

package com.github.pgasync.impl;

import com.github.pgasync.impl.message.Message;
import com.github.pgasync.impl.message.PasswordMessage;
import com.github.pgasync.impl.message.StartupMessage;
import io.reactivex.Completable;
import io.reactivex.Observable;

/**
 * Stream of messages from/to backend server.
 * 
 * @author Antti Laisi
 */
public interface PgProtocolStream {

    Observable<Message> connect(StartupMessage startup);

    Observable<Message> authenticate(PasswordMessage password);

    Observable<Message> send(Message... messages);

    Observable<String> listen(String channel);

    boolean isConnected();

    Completable close();

}
