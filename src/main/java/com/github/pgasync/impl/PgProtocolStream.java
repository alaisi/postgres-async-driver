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
import com.github.pgasync.impl.message.backend.CommandComplete;
import com.github.pgasync.impl.message.backend.DataRow;
import com.github.pgasync.impl.message.backend.RowDescription;
import com.github.pgasync.impl.message.frontend.Execute;
import com.github.pgasync.impl.message.frontend.PasswordMessage;
import com.github.pgasync.impl.message.frontend.Query;
import com.github.pgasync.impl.message.frontend.StartupMessage;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Stream of messages from/to backend server.
 *
 * @author Antti Laisi
 */
public interface PgProtocolStream {

    CompletableFuture<Message> connect(StartupMessage startup);

    CompletableFuture<Message> authenticate(PasswordMessage password);

    CompletableFuture<Message> send(Message message);

    CompletableFuture<Void> send(Query query, Consumer<RowDescription.ColumnDescription[]> onColumns, Consumer<DataRow> onRow, Consumer<CommandComplete> onAffected);

    CompletableFuture<Integer> send(Execute execute, Consumer<DataRow> onRow);

    Runnable subscribe(String channel, Consumer<String> onNotification);

    boolean isConnected();

    CompletableFuture<Void> close();

}
