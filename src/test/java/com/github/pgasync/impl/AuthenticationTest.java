package com.github.pgasync.impl;

import com.github.pgasync.ConnectionPool;
import com.github.pgasync.SqlException;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.github.pgasync.impl.DatabaseRule.createPoolBuilder;
import static org.junit.Assert.assertEquals;

public class AuthenticationTest {

    @Test
    public void shouldThrowExceptionOnInvalidCredentials() throws Exception {
        try (ConnectionPool pool = createPoolBuilder(1).password("_invalid_").build()) {
            BlockingQueue<Throwable> result = new ArrayBlockingQueue<>(1);
            pool.query("SELECT 1", System.out::println, result::add);
            SqlException sqle = (SqlException) result.poll(5, TimeUnit.SECONDS);
            assertEquals("28P01", sqle.getCode());
        }
    }

}
