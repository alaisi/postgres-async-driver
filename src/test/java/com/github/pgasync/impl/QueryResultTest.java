package com.github.pgasync.impl;

import static org.junit.Assert.assertEquals;

import java.util.Iterator;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.pgasync.ResultSet;
import com.github.pgasync.Row;
import com.github.pgasync.SqlException;

/**
 * Tests for result set row counts.
 * 
 * @author Antti Laisi
 */
public class QueryResultTest extends ConnectedTest {

    @BeforeClass
    public static void create() {
        query("CREATE TABLE CONN_TEST(ID INT8)");
    }

    @AfterClass
    public static void drop() {
        query("DROP TABLE IF EXISTS CONN_TEST");
    }

    @Test
    public void shouldReturnResultSetSize() {
        assertEquals(2, query("INSERT INTO CONN_TEST (ID) VALUES (1),(2)").updatedRows());
        ResultSet result = query("SELECT * FROM CONN_TEST WHERE ID <= 2 ORDER BY ID");
        assertEquals(2, result.size());

        Iterator<Row> i = result.iterator();
        assertEquals(1L, i.next().getLong(0).longValue());
        assertEquals(2L, i.next().getLong(0).longValue());
    }

    @Test
    public void shouldReturnDeletedRowsCount() {
        assertEquals(1, query("INSERT INTO CONN_TEST (ID) VALUES (3)").updatedRows());
        assertEquals(1, query("DELETE FROM CONN_TEST WHERE ID = 3").updatedRows());
    }

    @Test
    public void shouldReturnUpdatedRowsCount() {
        assertEquals(3, query("INSERT INTO CONN_TEST (ID) VALUES (9),(10),(11)").updatedRows());
        assertEquals(3, query("UPDATE CONN_TEST SET ID = NULL WHERE ID IN (9,10,11)").updatedRows());
    }

    @Test(expected = SqlException.class)
    public void shouldInvokeErrorHandlerOnError() {
        query("SELECT * FROM not_there");
    }

}
