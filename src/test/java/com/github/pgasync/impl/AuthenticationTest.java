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
        ConnectionPool pool = dbr.builder.password("_invalid_").build();
        try {
            pool.completeQuery("SELECT 1").get();
            fail();
        } catch (Exception ex) {
            SqlException sqlException = (SqlException) ex;
            assertEquals("28P01", sqlException.getCode());
        } finally {
            pool.close().get();
        }
    }

}
