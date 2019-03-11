package com.github.pgasync;

import com.pgasync.Connectible;
import com.pgasync.Connection;
import com.pgasync.PreparedStatement;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * @author Marat Gainullin
 */
public class PreparedStatementsCacheTest {

    private static final String SELECT_52 = "select 52";
    private static final String SELECT_32 = "select 32";

    @ClassRule
    public static DatabaseRule dbr = new DatabaseRule(DatabaseRule.createPoolBuilder(5));

    private Connectible pool;

    @Before
    public void setup() {
        pool = dbr.builder
                .maxConnections(1)
                .maxStatements(1)
                .pool();
    }

    @After
    public void shutdown() {
        pool.close().join();
    }

    @Test
    public void shouldEvictedStatementBeReallyClosed() {
        Connection conn = pool.getConnection().join();
        try {
            PreparedStatement evictor = conn.prepareStatement(SELECT_52).join();
            try {
                PreparedStatement evicted = conn.prepareStatement(SELECT_32).join();
                evicted.close().join();
            } finally {
                evictor.close().join();
            }
        } finally {
            conn.close().join();
        }
    }

    @Test
    public void shouldDuplicatedStatementBeReallyClosed() {
        Connection conn = pool.getConnection().join();
        try {
            PreparedStatement stmt = conn.prepareStatement(SELECT_52).join();
            try {
                PreparedStatement duplicated = conn.prepareStatement(SELECT_52).join();
                duplicated.close().join();
            } finally {
                stmt.close().join();
            }
        } finally {
            conn.close().join();
        }
    }

    @Test
    public void shouldDuplicatedAndEvictedStatementsBeReallyClosed() {
        Connection conn = pool.getConnection().join();
        try {
            PreparedStatement stmt = conn.prepareStatement(SELECT_52).join();
            try {
                PreparedStatement duplicated = conn.prepareStatement(SELECT_52).join();
                try {
                    PreparedStatement evicted = conn.prepareStatement(SELECT_32).join();
                    evicted.close().join();
                } finally {
                    duplicated.close().join();
                }
            } finally {
                stmt.close().join();
            }
        } finally {
            conn.close().join();
        }
    }

}
