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

import java.net.InetSocketAddress;

import com.github.pgasync.callback.ConnectionHandler;
import com.github.pgasync.callback.ErrorHandler;
import com.github.pgasync.impl.netty.NettyPgConnectionPool;

/**
 * Pool of backend {@link Connection}s.
 * 
 * @author Antti Laisi
 */
public interface ConnectionPool extends Connection {

    /**
     * Executes a {@link ConnectionHandler} callback when a connection is
     * available.
     * 
     * @param handler Called when a connection is acquired
     * @param onError Called on exception thrown
     */
    void getConnection(ConnectionHandler handler, ErrorHandler onError);

    /**
     * Builder
     */
    public static class Builder {

        String hostname = "localhost";
        int port = 5432;
        String username;
        String password;
        String database;
        int poolSize = 20;

        public Builder hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder poolSize(int poolSize) {
            this.poolSize = poolSize;
            return this;
        }

        public ConnectionPool build() {
            return new NettyPgConnectionPool(new InetSocketAddress(hostname, port), 
                    username, password, database, poolSize);
        }
    }

}
