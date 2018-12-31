package com.github.pgasync.impl;

import com.github.pgasync.ConnectionPool;
import com.github.pgasync.SqlException;
import org.junit.ClassRule;
import org.junit.Test;

import static com.github.pgasync.impl.DatabaseRule.createPoolBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AuthenticationTest {

    @ClassRule
    public static DatabaseRule dbr = new DatabaseRule(createPoolBuilder(1));

    @Test
    public void shouldThrowExceptionOnInvalidCredentials() throws Exception {
        try (ConnectionPool pool = dbr.builder.password("_invalid_").build()) {
            pool.queryRows("SELECT 1").get();
            fail();
        } catch (SqlException sqle) {
            assertEquals("28P01", sqle.getCode());
        }
    }

}
