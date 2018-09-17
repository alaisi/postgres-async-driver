package com.github.pgasync.impl;

import com.github.pgasync.Connection;
import com.github.pgasync.ResultSet;
import com.github.pgasync.SqlException;
import org.junit.Test;
import rx.Observable;

import java.util.function.Consumer;

import static org.junit.Assert.*;

public class ConnectionValidatorTest {

    @Test
    public void shouldBeTheSamePidOnSuccessiveCalls() {
        withDbr(null, true, dbr -> {
            // Simple sanity check for our PID assumptions
            assertEquals(selectPid(dbr).toBlocking().single().intValue(),
                    selectPid(dbr).toBlocking().single().intValue());
        });
    }

    @Test
    public void shouldBeSamePidWhenValidationQuerySucceeds() {
        withDbr("SELECT 1", false, dbr -> {
            // Just compare PIDs
            assertEquals(selectPid(dbr).toBlocking().single().intValue(),
                    selectPid(dbr).toBlocking().single().intValue());
        });
    }

    @Test
    public void shouldFailValidationQueryFailsAndReconnectAfterSuccess() throws Exception {
        String errSql =
                "DO language plpgsql $$\n" +
                "  BEGIN\n" +
                "    IF (SELECT COUNT(1) FROM VSTATE) = 1 THEN\n" +
                "      RAISE 'ERR';\n" +
                "    END IF;\n" +
                "  EXCEPTION\n" +
                "    WHEN undefined_table THEN\n" +
                "  END\n" +
                "$$;";
        withDbr(errSql, false, dbr -> {
            // Add the VSTATE table
            dbr.query("DROP TABLE IF EXISTS VSTATE; CREATE TABLE VSTATE (ID VARCHAR(255) PRIMARY KEY)");

            try {
                // Grab the pid
                int pid = selectPid(dbr).toBlocking().single();

                // Break it
                runFromOutside(dbr, "INSERT INTO VSTATE VALUES('A')");

                // Make sure it is broken
                try {
                    selectPid(dbr).toBlocking().single();
                    fail("Should be broken");
                } catch (SqlException e) { }

                // Fix it, and go ahead and expect the same PID
                runFromOutside(dbr, "TRUNCATE TABLE VSTATE");
                assertEquals(pid, selectPid(dbr).toBlocking().single().intValue());
            } finally {
                runFromOutside(dbr, "DROP TABLE IF EXISTS VSTATE");
            }
        });
    }

    @Test
    public void shouldErrorWhenNotValidatingSocket() {
        withDbr(null, false, dbr -> {
            // Simple check, kill from outside, confirm failure
            assertNotNull(selectPid(dbr).toBlocking().single());
            killConnectionFromOutside(dbr);
            try {
                selectPid(dbr).toBlocking().single();
                fail("Should not succeed after killing connection");
            } catch (IllegalStateException e) { }
        });
    }

    @Test
    public void shouldNotErrorWhenValidatingSocket() {
        withDbr(null, true, dbr -> {
            // Grab pid, kill from outside, confirm different pid
            int pid = selectPid(dbr).toBlocking().single();
            killConnectionFromOutside(dbr);
            assertNotEquals(pid, selectPid(dbr).toBlocking().single().intValue());
        });
    }

    private static Observable<Integer> selectPid(DatabaseRule dbr) {
        return dbr.db().queryRows("SELECT pg_backend_pid()").map(r -> r.getInt(0));
    }

    private static void killConnectionFromOutside(DatabaseRule dbr) {
        ResultSet rs = runFromOutside(dbr, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity " +
                "WHERE pid <> pg_backend_pid() AND datname = '" + ((PgConnectionPool) dbr.pool).database + "'");
        assertEquals(1, rs.size());
        // Unfortunately, it appears we have to wait a tiny bit after
        // killing the connection for netty to know
        try { Thread.sleep(300); } catch (Exception e) { }
    }

    private static ResultSet runFromOutside(DatabaseRule dbr, String query) {
        PgConnectionPool pool = (PgConnectionPool) dbr.pool;
        try(Connection conn = new PgConnection(pool.openStream(pool.address), pool.dataConverter).
                connect(pool.username, pool.password, pool.database).toBlocking().single()) {
            return conn.querySet(query).toBlocking().single();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void withDbr(String validationQuery, boolean validateSocket, Consumer<DatabaseRule> fn) {
        DatabaseRule rule = new DatabaseRule();
        rule.builder.validationQuery(validationQuery);
        rule.builder.validateSocket(validateSocket);
        rule.before();
        try {
            fn.accept(rule);
        } finally {
            rule.after();
        }
    }
}
