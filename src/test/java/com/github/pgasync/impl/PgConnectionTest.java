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
 * Test for a single backend connection.
 *
 * @author Antti Laisi
 */
public class PgConnectionTest extends ConnectedTest {

	@BeforeClass
	public static void create() {
		query("CREATE TABLE CONN_TEST(ID INT8)");
	}

	@AfterClass
	public static void drop() {
		query("DROP TABLE IF EXISTS CONN_TEST");
	}

	@Test
	public void shouldReturnMultipleRows() {
		assertEquals(2, query("INSERT INTO CONN_TEST (ID) VALUES (1),(2)").updatedRows());
		ResultSet result = query("SELECT * FROM CONN_TEST WHERE ID <= 2 ORDER BY ID");
		assertEquals(2, result.size());

		Iterator<Row> i = result.iterator();
		assertEquals(1L, i.next().getLong(0).longValue());
		assertEquals(2L, i.next().getLong(0).longValue());
	}

	@Test
	public void shouldDeleteRows() {
		assertEquals(1, query("INSERT INTO CONN_TEST (ID) VALUES (3)").updatedRows());
	}

	@Test(expected=SqlException.class)
	public void shouldInvokeErrorHandlerOnError() {
		query("SELECT * FROM not_there");
	}

}
