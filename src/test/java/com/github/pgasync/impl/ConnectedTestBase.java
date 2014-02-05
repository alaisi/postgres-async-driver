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

import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.github.pgasync.ConnectionPoolBuilder;
import com.github.pgasync.Connection;
import com.github.pgasync.ConnectionPool;
import com.github.pgasync.ResultSet;

/**
 * Base class for tests connected to a PostgreSQL backend.
 * 
 * @author Antti Laisi
 */
public abstract class ConnectedTestBase {

    // Use ThreadLocal so tests can be run concurrently
    static ThreadLocal<ConnectionPool> pool = new ThreadLocal<>();

    @BeforeClass
    public static void createConnection() {
        pool.set(createPool(1));
    }

    @AfterClass
    public static void closeConnection() {
        ConnectionPool p = pool.get();
        if(p != null) {
            p.close();
        }
        pool.remove();
    }

    protected static ResultSet query(String sql) {
        ResultHolder result = new ResultHolder();
        connection().query(sql, result, result);
        return result.result();
    }

    @SuppressWarnings("rawtypes")
    protected static ResultSet query(String sql, List/* <Object> */params) {
        ResultHolder result = new ResultHolder();
        connection().query(sql, params, result, result);
        return result.result();
    }

    protected static ConnectionPool createPool(int size) {
        return new ConnectionPoolBuilder()
            .database(envOrDefault("PG_DATABASE", "postgres"))
            .username(envOrDefault("PG_USERNAME", "postgres"))
            .password(envOrDefault("PG_PASSWORD", "postgres"))
            .poolSize(size)
            .build();
    }

    protected static Connection connection() {
        return pool.get();
    }

    static String envOrDefault(String var, String def) {
        String value = System.getenv(var);
        return value != null ? value : def;
    }
}
