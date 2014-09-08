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
public class DatabaseRule extends ExternalResource {

    ConnectionPool pool;

    @Override
    protected void before() throws Throwable {
        pool = createPool(1);
    }

    @Override
    protected void after() {
        if(pool != null) {
            pool.close();
        }
    }

    ResultSet query(String sql) {
        ResultHolder result = new ResultHolder();
        db().query(sql, result, result);
        return result.result();
    }

    @SuppressWarnings("rawtypes")
    ResultSet query(String sql, List/*<Object>*/ params) {
        ResultHolder result = new ResultHolder();
        db().query(sql, params, result, result);
        return result.result();
    }

    Db db() {
        return pool;
    }

    static ConnectionPool createPool(int size) {
        return new ConnectionPoolBuilder()
                .database(envOrDefault("PG_DATABASE", "postgres"))
                .username(envOrDefault("PG_USERNAME", "postgres"))
                .password(envOrDefault("PG_PASSWORD", "postgres"))
                .poolSize(size)
                .build();
    }

    static String envOrDefault(String var, String def) {
        String value = System.getenv(var);
        return value != null ? value : def;
    }
}
