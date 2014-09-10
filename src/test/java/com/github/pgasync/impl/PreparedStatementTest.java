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

import static com.github.pgasync.impl.io.IO.bytes;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.sql.Date;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

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
                + "BYTEA BYTEA" + ")");
    }

    @AfterClass
    public static void drop() {
        dbr.query("DROP TABLE IF EXISTS PS_TEST");
    }

    @Test
    public void shouldBindLong() {
        dbr.query("INSERT INTO PS_TEST(LONG) VALUES ($1)", asList(Long.MIN_VALUE));
        assertEquals(Long.MIN_VALUE, dbr.query("SELECT LONG FROM PS_TEST WHERE LONG = $1", asList(Long.MIN_VALUE))
                .get(0).getLong(0).longValue());
    }

    @Test
    public void shouldBindInt() {
        dbr.query("INSERT INTO PS_TEST(INT) VALUES ($1)", asList(Integer.MAX_VALUE));
        assertEquals(Integer.MAX_VALUE, dbr.query("SELECT INT FROM PS_TEST WHERE INT = $1", asList(Integer.MAX_VALUE))
                .get(0).getInt(0).intValue());
    }

    @Test
    public void shouldBindShort() {
        dbr.query("INSERT INTO PS_TEST(SHORT) VALUES ($1)", asList(Short.MIN_VALUE));
        assertEquals(Short.MIN_VALUE, dbr.query("SELECT SHORT FROM PS_TEST WHERE SHORT = $1", asList(Short.MIN_VALUE))
                .get(0).getLong(0).shortValue());
    }

    @Test
    public void shouldBindByte() {
        dbr.query("INSERT INTO PS_TEST(BYTE) VALUES ($1)", asList(0x41));
        assertEquals(0x41, dbr.query("SELECT BYTE FROM PS_TEST WHERE BYTE = $1", asList(0x41)).get(0).getByte(0)
                .byteValue());
    }

    @Test
    public void shouldBindChar() {
        dbr.query("INSERT INTO PS_TEST(CHAR) VALUES ($1)", asList('€'));
        assertEquals('€', dbr.query("SELECT CHAR FROM PS_TEST WHERE CHAR = $1", asList('€')).get(0).getChar(0)
                .charValue());
    }

    @Test
    public void shouldBindString() {
        dbr.query("INSERT INTO PS_TEST(STRING) VALUES ($1)", asList("val"));
        assertEquals("val",
                dbr.query("SELECT STRING FROM PS_TEST WHERE STRING = $1", asList("val")).get(0).getString(0));
    }

    @Test
    public void shouldBindClob() {
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            s.append(getClass());
        }
        String text = s.toString();
        dbr.query("INSERT INTO PS_TEST(CLOB) VALUES ($1)", asList(text));
        assertEquals(text, dbr.query("SELECT CLOB FROM PS_TEST WHERE CLOB = $1", asList(text)).get(0).getString(0));
    }

    @Test
    public void shouldBindTime() throws Exception {
        Time time = new Time(dateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse("1970-01-01 16:47:59.897")
                .getTime());
        dbr.query("INSERT INTO PS_TEST(TIME) VALUES ($1)", asList(time));
        assertEquals(time, dbr.query("SELECT TIME FROM PS_TEST WHERE TIME = $1", asList(time)).get(0).getTime(0));
    }

    @Test
    public void shouldBindDate() throws Exception {
        Date date = new Date(dateFormat("yyyy-MM-dd").parse("2014-01-19").getTime());
        dbr.query("INSERT INTO PS_TEST(DATE) VALUES ($1)", asList(date));
        assertEquals(date, dbr.query("SELECT DATE FROM PS_TEST WHERE DATE = $1", asList(date)).get(0).getDate(0));
    }

    @Test
    public void shouldBindBytes() throws Exception {
        byte[] b = bytes("blob content");
        dbr.query("INSERT INTO PS_TEST(BYTEA) VALUES ($1)", asList(b));
        assertArrayEquals(b, dbr.query("SELECT BYTEA FROM PS_TEST WHERE BYTEA = $1", asList(b)).get(0).getBytes(0));
    }

    static SimpleDateFormat dateFormat(String pattern) {
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        format.setTimeZone(TimeZone.getTimeZone("UTC"));
        return format;
    }
}
