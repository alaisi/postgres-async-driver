package com.github.pgasync.impl;

import static com.github.pgasync.impl.io.IO.bytes;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.sql.Date;
import java.sql.Time;
import java.text.SimpleDateFormat;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for parameter binding.
 * 
 * @author Antti Laisi
 */
public class PreparedStatementTest extends ConnectedTest {

	@BeforeClass
	public static void create() {
		query("CREATE TABLE PS_TEST(" 
				+ "LONG INT8,INT INT4,SHORT INT2, BYTE INT2," 
				+ "CHAR CHAR(1), STRING VARCHAR(255), CLOB TEXT,"
				+ "TIME TIME, DATE DATE, TS TIMESTAMP,"
				+ "BYTEA BYTEA"
			+ ")");
	}

	@AfterClass
	public static void drop() {
		query("DROP TABLE IF EXISTS PS_TEST");
	}

	@Test
	public void shouldBindLong() {
		query("INSERT INTO PS_TEST(LONG) VALUES ($1)", asList(Long.MIN_VALUE));
		assertEquals(Long.MIN_VALUE, 
				query("SELECT LONG FROM PS_TEST WHERE LONG = $1", asList(Long.MIN_VALUE)).get(0).getLong(0).longValue());
	}

	@Test
	public void shouldBindInt() {
		query("INSERT INTO PS_TEST(INT) VALUES ($1)", asList(Integer.MAX_VALUE));
		assertEquals(Integer.MAX_VALUE, 
				query("SELECT INT FROM PS_TEST WHERE INT = $1", asList(Integer.MAX_VALUE)).get(0).getInt(0).intValue());
	}

	@Test
	public void shouldBindShort() {
		query("INSERT INTO PS_TEST(SHORT) VALUES ($1)", asList(Short.MIN_VALUE));
		assertEquals(Short.MIN_VALUE, 
				query("SELECT SHORT FROM PS_TEST WHERE SHORT = $1", asList(Short.MIN_VALUE)).get(0).getLong(0).shortValue());
	}

	@Test
	public void shouldBindByte() {
		query("INSERT INTO PS_TEST(BYTE) VALUES ($1)", asList(0x41));
		assertEquals(0x41, 
				query("SELECT BYTE FROM PS_TEST WHERE BYTE = $1", asList(0x41)).get(0).getByte(0).byteValue());
	}

	@Test
	public void shouldBindChar() {
		query("INSERT INTO PS_TEST(CHAR) VALUES ($1)", asList('€'));
		assertEquals('€', 
				query("SELECT CHAR FROM PS_TEST WHERE CHAR = $1", asList('€')).get(0).getChar(0).charValue());
	}

	@Test
	public void shouldBindString() {
		query("INSERT INTO PS_TEST(STRING) VALUES ($1)", asList("val"));
		assertEquals("val", 
				query("SELECT STRING FROM PS_TEST WHERE STRING = $1", asList("val")).get(0).getString(0));
	}

	@Test
	public void shouldBindClob() {
		StringBuilder s = new StringBuilder();
		for(int i = 0; i < 10000; i++) {
			s.append(getClass());
		}
		String text = s.toString();
		query("INSERT INTO PS_TEST(CLOB) VALUES ($1)", asList(text));
		assertEquals(text, 
				query("SELECT CLOB FROM PS_TEST WHERE CLOB = $1", asList(text)).get(0).getString(0));
	}

	@Test
	public void shouldBindTime() throws Exception {
		Time time = new Time(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse("1970-01-01 16:47:59.897").getTime());
		query("INSERT INTO PS_TEST(TIME) VALUES ($1)", asList(time));
		assertEquals(time, query("SELECT TIME FROM PS_TEST WHERE TIME = $1", asList(time)).get(0).getTime(0));
	}

	@Test
	public void shouldBindDate() throws Exception  {
		Date date = new Date(new SimpleDateFormat("yyyy-MM-dd").parse("2014-01-19").getTime());
		query("INSERT INTO PS_TEST(DATE) VALUES ($1)", asList(date));
		assertEquals(date, query("SELECT DATE FROM PS_TEST WHERE DATE = $1", asList(date)).get(0).getDate(0));
	}

	@Test
	public void shouldBindBytes() throws Exception  {
		byte[] b = bytes("blob content");
		query("INSERT INTO PS_TEST(BYTEA) VALUES ($1)", asList(b));
		assertArrayEquals(b, query("SELECT BYTEA FROM PS_TEST WHERE BYTEA = $1", asList(b)).get(0).getBytes(0));
	}

}
