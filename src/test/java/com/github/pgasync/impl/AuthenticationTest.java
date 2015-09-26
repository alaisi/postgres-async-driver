package com.github.pgasync.impl;

import com.github.pgasync.ConnectionPool;
import com.github.pgasync.SqlException;
import org.junit.Test;

import static com.github.pgasync.impl.DatabaseRule.createPoolBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AuthenticationTest {

    @Test
    public void shouldThrowExceptionOnInvalidCredentials() throws Exception {
        try (ConnectionPool pool = createPoolBuilder(1).password("_invalid_").build()) {
            pool.queryRows("SELECT 1").toBlocking().first();
            fail();
        } catch (SqlException sqle) {
            assertEquals("28P01", sqle.getCode());
        }
    }

}
