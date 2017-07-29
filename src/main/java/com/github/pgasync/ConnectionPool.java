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

import io.reactivex.Single;

/**
 * Pool of backend {@link Connection}s. Pools implement {@link Db} so
 * queries can be issued directly to the pool if using the same connection
 * is not required.
 * 
 * @author Antti Laisi
 */
public interface ConnectionPool extends Db {

    /**
     * Executes a {@link java.util.function.Consumer} callback when a connection is
     * available. Connection passed to callback must be freed with
     * {@link com.github.pgasync.ConnectionPool#release(Connection)}
     */
    Single<Connection> getConnection();

    /**
     * Releases a connection back to the pool.
     * 
     * @param connection Connection fetched with getConnection
     */
    void release(Connection connection);

}
