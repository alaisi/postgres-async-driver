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

import com.github.pgasync.impl.ConnectionValidator;
import com.github.pgasync.impl.conversion.DataConverter;
import com.github.pgasync.impl.netty.NettyPgConnectionPool;
import rx.Observable;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
        properties.hostname = hostname;
        return this;
    }

    public ConnectionPoolBuilder port(int port) {
        properties.port = port;
        return this;
    }

    public ConnectionPoolBuilder username(String username) {
        properties.username = username;
        return this;
    }

    public ConnectionPoolBuilder password(String password) {
        properties.password = password;
        return this;
    }

    public ConnectionPoolBuilder database(String database) {
        properties.database = database;
        return this;
    }

    public ConnectionPoolBuilder poolSize(int poolSize) {
        properties.poolSize = poolSize;
        return this;
    }

    public ConnectionPoolBuilder converters(Converter<?>... converters) {
        Collections.addAll(properties.converters, converters);
        return this;
    }

    public ConnectionPoolBuilder dataConverter(DataConverter dataConverter) {
        properties.dataConverter = dataConverter;
        return this;
    }

    public ConnectionPoolBuilder ssl(boolean ssl) {
        properties.useSsl = ssl;
        return this;
    }

    public ConnectionPoolBuilder pipeline(boolean pipeline) {
        properties.usePipelining = pipeline;
        return this;
    }

    public ConnectionPoolBuilder validationQuery(String validationQuery) {
        properties.validationQuery = validationQuery;
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
        DataConverter dataConverter = null;
        List<Converter<?>> converters = new ArrayList<>();
        boolean useSsl;
        boolean usePipelining;
        String validationQuery;

        public String getHostname() {
            return hostname;
        }
        public int getPort() {
            return port;
        }
        public String getUsername() {
            return username;
        }
        public String getPassword() {
            return password;
        }
        public String getDatabase() {
            return database;
        }
        public int getPoolSize() {
            return poolSize;
        }
        public boolean getUseSsl() {
            return useSsl;
        }
        public boolean getUsePipelining() {
            return usePipelining;
        }
        public DataConverter getDataConverter() {
            return dataConverter != null ? dataConverter : new DataConverter(converters);
        }
        public Func1<Connection,Observable<Connection>> getValidator() {
            return validationQuery == null || validationQuery.trim().isEmpty()
                ? Observable::just
                : new ConnectionValidator(validationQuery)::validate;
        }
    }
}