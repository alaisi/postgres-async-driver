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

package com.github.pgasync;

import com.github.pgasync.impl.Oid;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * A single physical connection to Postgres backend.
 *
 * @author Antti Laisi
 */
public interface Connection extends Db {

    CompletableFuture<PreparedStatement> prepareStatement(String sql, Oid... parametersTypes);

    CompletableFuture<Listening> subscribe(String channel, Consumer<String> onNotification);
}