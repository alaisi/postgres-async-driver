package com.github.pgasync;

import com.pgasync.Connectible;
import com.pgasync.ResultSet;
import com.pgasync.SqlException;
import org.junit.ClassRule;
import org.junit.Test;

import static com.github.pgasync.DatabaseRule.createPoolBuilder;
import static org.junit.Assert.assertEquals;

public class AuthenticationTest {

    @ClassRule
    public static DatabaseRule dbr = new DatabaseRule(createPoolBuilder(1));

    @Test(expected = SqlException.class)
    public void shouldThrowExceptionOnInvalidCredentials() throws Exception {
        Connectible pool = dbr.builder
                .password("_invalid_")
                .pool();
        try {
            pool.completeQuery("SELECT 1").get();
        } catch (Exception ex) {
            SqlException.ifCause(ex,
                    sqlException -> {
                        assertEquals("28P01", sqlException.getCode());
                        throw sqlException;
                    },
                    () -> {
                        throw ex;
                    });
        } finally {
            pool.close().get();
        }
    }

    @Test
    public void shouldGetResultOnValidCredentials() throws Exception {
        Connectible pool = dbr.builder
                .password("async-pg")
                .pool();
        try {
            ResultSet rs = pool.completeQuery("SELECT 1").get();
            assertEquals(1L, (long) rs.at(0).getInt(0));
        } finally {
            pool.close().get();
        }
    }

}
