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

import com.github.pgasync.Oid;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * A single physical connection to Postgres backend.
 *
 * Concurrent using of implementations is impossible.
 * {@link Connection} implementations are never thread-safe.
 * They are designed to be used in context of single {@link CompletableFuture} completion at a time.
 *
 * @author Antti Laisi
 * @author Marat Gainullin
 */
public interface Connection extends QueryExecutor {

    CompletableFuture<PreparedStatement> prepareStatement(String sql, Oid... parametersTypes);

    /**
     * The typical scenario of using notifications is as follows:
     * - {@link #subscribe(String, Consumer)}
     * - Some calls of @onNotification callback
     * - {@link Listening#unlisten()}
     *
     * @param channel Channel name to listen to.
     * @param onNotification Callback, thar is cinvoked every time notification arrives.
     * @return CompletableFuture instance, completed when subscription will be registered at the backend.
     */
    CompletableFuture<Listening> subscribe(String channel, Consumer<String> onNotification);

    CompletableFuture<Void> close();
}