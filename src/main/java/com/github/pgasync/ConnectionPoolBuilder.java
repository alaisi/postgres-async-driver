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

import com.github.pgasync.impl.netty.NettyPgConnectionPool;

/**
 * Builder for creating {@link ConnectionPool} instances.
 * 
 * @author Antti Laisi
 */
public class ConnectionPoolBuilder {

    final PoolProperties properties = new PoolProperties();

    /**
     * @return Pool ready for use
     */
    public ConnectionPool build() {
        return new NettyPgConnectionPool(properties);
    }

    public ConnectionPoolBuilder hostname(String hostname) {
        properties.setHostname(hostname);
        return this;
    }

    public ConnectionPoolBuilder port(int port) {
        properties.setPort(port);
        return this;
    }

    public ConnectionPoolBuilder username(String username) {
        properties.setUsername(username);
        return this;
    }

    public ConnectionPoolBuilder password(String password) {
        properties.setPassword(password);
        return this;
    }

    public ConnectionPoolBuilder database(String database) {
        properties.setDatabase(database);
        return this;
    }

    public ConnectionPoolBuilder poolSize(int poolSize) {
        properties.setPoolSize(poolSize);
        return this;
    }

    /**
     * Configuration for a pool.
     */
    public static class PoolProperties {

        String hostname = "localhost";
        int port = 5432;
        String username;
        String password;
        String database;
        int poolSize = 20;

        public String getHostname() {
            return hostname;
        }
        public void setHostname(String hostname) {
            this.hostname = hostname;
        }
        public int getPort() {
            return port;
        }
        public void setPort(int port) {
            this.port = port;
        }
        public String getUsername() {
            return username;
        }
        public void setUsername(String username) {
            this.username = username;
        }
        public String getPassword() {
            return password;
        }
        public void setPassword(String password) {
            this.password = password;
        }
        public String getDatabase() {
            return database;
        }
        public void setDatabase(String database) {
            this.database = database;
        }
        public int getPoolSize() {
            return poolSize;
        }
        public void setPoolSize(int poolSize) {
            this.poolSize = poolSize;
        }
    }
}