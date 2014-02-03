package com.github.pgasync.impl;

import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.github.pgasync.Connection;
import com.github.pgasync.ConnectionPool;
import com.github.pgasync.ResultSet;

public abstract class ConnectedTest {

    static ConnectionPool pool;

    @BeforeClass
    public static void createConnection() {
        pool = createPool(1);
    }

    @AfterClass
    public static void closeConnection() {
        pool.close();
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
        return new ConnectionPool.Builder()
            .database(envOrDefault("PG_DATABASE", "postgres"))
            .username(envOrDefault("PG_USERNAME", "postgres"))
            .password(envOrDefault("PG_PASSWORD", "postgres"))
            .poolSize(size)
            .build();
    }

    protected static Connection connection() {
        return pool;
    }

    static String envOrDefault(String var, String def) {
        String value = System.getenv(var);
        return value != null ? value : def;
    }
}
