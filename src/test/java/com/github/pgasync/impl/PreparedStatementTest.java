/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.pgasync.impl;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static com.github.pgasync.impl.io.IO.bytes;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

/**
 * Tests for parameter binding.
 * 
 * @author Antti Laisi
 */
public class PreparedStatementTest {

    @ClassRule
    public static DatabaseRule dbr = new DatabaseRule();

    @BeforeClass
    public static void create() {
        drop();
        dbr.query("CREATE TABLE PS_TEST("
                + "LONG INT8,INT INT4,SHORT INT2, BYTE INT2,"
                + "CHAR CHAR(1), STRING VARCHAR(255), CLOB TEXT,"
                + "TIME TIME, DATE DATE, TS TIMESTAMP,"
                + "BYTEA BYTEA, BOOLEAN BOOLEAN)");
    }

    @AfterClass
    public static void drop() {
        dbr.query("DROP TABLE IF EXISTS PS_TEST");
    }

    @Test
    public void shouldBindLong() {
        dbr.query("INSERT INTO PS_TEST(LONG) VALUES ($1)", asList(Long.MIN_VALUE));
        assertEquals(Long.MIN_VALUE, dbr.query("SELECT LONG FROM PS_TEST WHERE LONG = $1", asList(Long.MIN_VALUE))
                .row(0).getLong(0).longValue());
    }

    @Test
    public void shouldBindInt() {
        dbr.query("INSERT INTO PS_TEST(INT) VALUES ($1)", asList(Integer.MAX_VALUE));
        assertEquals(Integer.MAX_VALUE, dbr.query("SELECT INT FROM PS_TEST WHERE INT = $1", asList(Integer.MAX_VALUE))
                .row(0).getInt(0).intValue());
    }

    @Test
    public void shouldBindShort() {
        dbr.query("INSERT INTO PS_TEST(SHORT) VALUES ($1)", asList(Short.MIN_VALUE));
        assertEquals(Short.MIN_VALUE, dbr.query("SELECT SHORT FROM PS_TEST WHERE SHORT = $1", asList(Short.MIN_VALUE))
                .row(0).getLong(0).shortValue());
    }

    @Test
    public void shouldBindByte() {
        dbr.query("INSERT INTO PS_TEST(BYTE) VALUES ($1)", asList(0x41));
        assertEquals(0x41, dbr.query("SELECT BYTE FROM PS_TEST WHERE BYTE = $1", asList(0x41)).row(0).getByte(0)
                .byteValue());
    }

    @Test
    public void shouldBindChar() {
        dbr.query("INSERT INTO PS_TEST(CHAR) VALUES ($1)", asList('€'));
        assertEquals('€', dbr.query("SELECT CHAR FROM PS_TEST WHERE CHAR = $1", asList('€')).row(0).getChar(0)
                .charValue());
    }

    @Test
    public void shouldBindString() {
        dbr.query("INSERT INTO PS_TEST(STRING) VALUES ($1)", asList("val"));
        assertEquals("val",
                dbr.query("SELECT STRING FROM PS_TEST WHERE STRING = $1", asList("val")).row(0).getString(0));
    }

    @Test
    public void shouldBindClob() {
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            s.append(getClass());
        }
        String text = s.toString();
        dbr.query("INSERT INTO PS_TEST(CLOB) VALUES ($1)", asList(text));
        assertEquals(text, dbr.query("SELECT CLOB FROM PS_TEST WHERE CLOB = $1", asList(text)).row(0).getString(0));
    }

    @Test
    public void shouldBindTime() throws Exception {
        Time time = Time.valueOf(LocalTime.parse("16:47:59.897"));
        dbr.query("INSERT INTO PS_TEST(TIME) VALUES ($1)", asList(time));
        assertEquals(time, dbr.query("SELECT TIME FROM PS_TEST WHERE TIME = $1", asList(time)).row(0).getTime(0));
    }

    @Test
    public void shouldBindTimestamp() throws Exception {
        Timestamp ts = Timestamp.valueOf(LocalDateTime.parse("2016-05-01T12:00:00"));
        dbr.query("INSERT INTO PS_TEST(TS) VALUES ($1)", singletonList(ts));
        assertEquals(ts, dbr.query("SELECT TS FROM PS_TEST WHERE TS = $1", asList(ts)).row(0).getTimestamp(0));
    }

    @Test
    public void shouldBindDate() throws Exception {
        Date date = Date.valueOf(LocalDate.parse("2014-01-19"));
        dbr.query("INSERT INTO PS_TEST(DATE) VALUES ($1)", asList(date));
        assertEquals(date, dbr.query("SELECT DATE FROM PS_TEST WHERE DATE = $1", asList(date)).row(0).getDate(0));
    }

    @Test
    public void shouldBindBytes() throws Exception {
        byte[] b = bytes("blob content");
        dbr.query("INSERT INTO PS_TEST(BYTEA) VALUES ($1)", asList(b));
        assertArrayEquals(b, dbr.query("SELECT BYTEA FROM PS_TEST WHERE BYTEA = $1", asList(b)).row(0).getBytes(0));
    }

    @Test
    public void shouldBindBoolean() throws Exception {
        dbr.query("INSERT INTO PS_TEST(BOOLEAN) VALUES ($1)", singletonList(true));
        assertTrue((Boolean) ((PgRow) dbr.query("SELECT BOOLEAN FROM PS_TEST WHERE BOOLEAN = $1",
                singletonList(true)).row(0)).get("BOOLEAN"));

    }

}
