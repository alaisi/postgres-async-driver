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
import com.github.pgasync.impl.message.StartupMessage;
import rx.Observable;

import java.util.function.Consumer;

/**
 * Stream of messages from/to backend server.
 * 
 * @author Antti Laisi
 */
public interface PgProtocolStream {

    Observable<Message> connect(StartupMessage startup);

    Observable<Message> send(Message... messages);

    boolean isConnected();

    void close();

    String registerNotificationHandler(String channel, Consumer<String> onNotification);

    void unRegisterNotificationHandler(String channel, String unlistenToken);

}
