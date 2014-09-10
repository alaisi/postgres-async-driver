package com.github.pgasync.impl;

import com.github.pgasync.ConnectionPool;
import com.github.pgasync.ConnectionPoolBuilder;
import com.github.pgasync.Db;
import com.github.pgasync.ResultSet;
import org.junit.rules.ExternalResource;

import java.util.List;

/**
 * @author Antti Laisi
 */
class DatabaseRule extends ExternalResource {

    final ConnectionPoolBuilder builder;
    ConnectionPool pool;

    DatabaseRule() {
        this(createPoolBuilder(1));
    }

    DatabaseRule(ConnectionPoolBuilder builder) {
        this.builder = builder;
    }

    @Override
    protected void before() {
        if(pool == null) {
            pool = builder.build();
        }
    }

    @Override
    protected void after() {
        if(pool != null) {
            pool.close();
        }
    }

    ResultSet query(String sql) {
        ResultHolder result = new ResultHolder();
        db().query(sql, result, result.errorHandler());
        return result.result();
    }

    @SuppressWarnings("rawtypes")
    ResultSet query(String sql, List/*<Object>*/ params) {
        ResultHolder result = new ResultHolder();
        db().query(sql, params, result, result.errorHandler());
        return result.result();
    }

    Db db() {
        before();
        return pool;
    }

    static ConnectionPool createPool(int size) {
        return createPoolBuilder(size).build();
    }
    static ConnectionPoolBuilder createPoolBuilder(int size) {
        return new ConnectionPoolBuilder()
                .database(envOrDefault("PG_DATABASE", "postgres"))
                .username(envOrDefault("PG_USERNAME", "postgres"))
                .password(envOrDefault("PG_PASSWORD", "postgres"))
                .poolSize(size);
    }


    static String envOrDefault(String var, String def) {
        String value = System.getenv(var);
        return value != null ? value : def;
    }
}
