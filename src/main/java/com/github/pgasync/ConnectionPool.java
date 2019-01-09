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

import java.util.concurrent.CompletableFuture;

/**
 * Pool of backend {@link Connection}s. Pools implement {@link Db} so
 * queries can be issued directly to the pool if using the same connection
 * is not required.
 *
 * @author Antti Laisi
 */
public interface ConnectionPool extends Db {

    /**
     * Gets a connection when available.
     * {@link Connection#close()} method will return the connection into this pool instead of closing it.
     *
     * @return {@link CompletableFuture} that is queued and completed when pool has an available connection.
     */
    CompletableFuture<Connection> getConnection();

}
