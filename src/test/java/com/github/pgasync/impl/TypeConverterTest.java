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

import org.junit.ClassRule;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Conversion tests from/to SQL types.
 * 
 * @author Antti Laisi
 */
public class TypeConverterTest {

    @ClassRule
    public static DatabaseRule dbr = new DatabaseRule();

    @Test
    public void shouldConvertNullToString() {
        assertNull(dbr.query("select NULL").get(0).getString(0));
    }

    @Test
    public void shouldConvertUnspecifiedToString() {
        assertEquals("test", dbr.query("select 'test'").get(0).getString(0));
    }

    @Test
    public void shouldConvertTextToString() {
        assertEquals("test", dbr.query("select 'test'::TEXT").get(0).getString(0));
    }

    @Test
    public void shouldConvertVarcharToString() {
        assertEquals("test", dbr.query("select 'test'::VARCHAR").get(0).getString(0));
    }

    @Test
    public void shouldConvertCharToString() throws Exception {
        assertEquals("test ", dbr.query("select 'test'::CHAR(5)").get(0).getString(0));
    }

    @Test
    public void shouldConvertSingleCharToString() {
        assertEquals("X", dbr.query("select 'X'::CHAR AS single").get(0).getString("single"));
    }

    @Test
    public void shouldConvertNullToLong() throws Exception {
        assertNull(dbr.query("select NULL").get(0).getLong(0));
    }

    @Test
    public void shouldConvertInt8ToLong() {
        assertEquals(5000L, dbr.query("select 5000::INT8").get(0).getLong(0).longValue());
    }

    @Test
    public void shouldConvertInt4ToLong() {
        assertEquals(4000L, dbr.query("select 4000::INT4 AS R").get(0).getLong("R").longValue());
    }

    @Test
    public void shouldConvertInt2ToLong() {
        assertEquals(4000L, dbr.query("select 4000::INT2").get(0).getLong(0).longValue());
    }

    @Test
    public void shouldConvertInt4ToInteger() {
        assertEquals(5000, dbr.query("select 5000::INT4 AS I").get(0).getInt("I").intValue());
    }

    @Test
    public void shouldConvertInt2ToInteger() {
        assertEquals(4000, dbr.query("select 4000::INT2").get(0).getInt(0).intValue());
    }

    @Test
    public void shouldConvertInt2ToShort() {
        assertEquals(3000, dbr.query("select 3000::INT2").get(0).getShort(0).shortValue());
    }

    @Test
    public void shouldConvertInt2ToShortWithName() {
        assertEquals(128, dbr.query("select 128::INT2 AS S").get(0).getShort("S").shortValue());
    }

    @Test
    public void shouldConvertCharToByte() {
        assertEquals(65, dbr.query("select 65::INT2").get(0).getByte(0).byteValue());
    }

    @Test
    public void shouldConvertCharToByteWithName() {
        assertEquals(65, dbr.query("select 65::INT2 as C").get(0).getByte("c").byteValue());
    }

    @Test
    public void shouldConvertInt8ToBigInteger() {
        assertEquals(new BigInteger("9223372036854775807"), dbr.query("select 9223372036854775807::INT8").get(0)
                .getBigInteger(0));
    }

    @Test
    public void shouldConvertInt4ToBigInteger() {
        assertEquals(new BigInteger("1000"), dbr.query("select 1000::INT4 as num").get(0).getBigInteger("num"));
    }

    @Test
    public void shouldConvertFloat8ToBigDecimal() {
        assertEquals(new BigDecimal("123.56"), dbr.query("select 123.56::FLOAT8").get(0).getBigDecimal(0));
    }

    @Test
    public void shouldConvertFloat4ToBigDecimal() {
        assertEquals(new BigDecimal("789.01"), dbr.query("select 789.01::FLOAT4 as sum").get(0).getBigDecimal("sum"));
    }

    @Test
    public void shouldConvertDateToDate() {
        assertEquals(new Date(millis(2014, 1, 31, 0, 0, 0, 0)),
                dbr.query("select '2014-01-31'::DATE").get(0).getDate(0));
    }

    @Test
    public void shouldConvertDateToDateWithName() {
        assertEquals(new Date(millis(2014, 2, 21, 0, 0, 0, 0)), dbr.query("select '2014-02-21'::DATE as D").get(0)
                .getDate("D"));
    }

    @Test
    public void shouldConvertTimeToTime() {
        assertEquals(new Time(millis(0, 0, 0, 10, 15, 31, 123)), dbr.query("select '10:15:31.123'::TIME").get(0)
                .getTime(0));
    }

    @Test
    public void shouldConvertZonedTimeToTime() {
        assertEquals(new Time(millis(0, 0, 0, 23, 59, 59, 999)), dbr.query("select '23:59:59.999Z'::TIMETZ as zoned")
                .get(0).getTime("zoned"));
    }

    @Test
    public void shouldConvertTimestampToTimestamp() {
        assertEquals(new Timestamp(millis(2014, 2, 21, 23, 59, 59, 999)), dbr.query("select '2014-02-21 23:59:59.999'::TIMESTAMP as ts")
                .get(0).getTimestamp("ts"));
    }

    @Test
    public void shouldConvertByteAToBytes() {
        assertArrayEquals(new byte[] { 0x41, 0x41 }, dbr.query("select '\\x4141'::BYTEA").get(0).getBytes(0));
    }

    @Test
    public void shouldConvertByteAToBytesWithName() {
        assertArrayEquals(new byte[] { 0x41, 0x41 }, dbr.query("select $1::BYTEA as bytes", asList("AA")).get(0)
                .getBytes("bytes"));
    }

    static long millis(int year, int month, int day, int hour, int minute, int second, int millisecond) {
        Calendar cal = Calendar.getInstance();
        cal.clear();
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        if (year > 0) {
            cal.set(Calendar.YEAR, year);
            cal.set(Calendar.MONTH, month - 1);
            cal.set(Calendar.DAY_OF_MONTH, day);
        }
        if (hour > 0) {
            cal.set(Calendar.HOUR_OF_DAY, hour);
            cal.set(Calendar.MINUTE, minute);
            cal.set(Calendar.SECOND, second);
            cal.set(Calendar.MILLISECOND, millisecond);
        }
        return cal.getTimeInMillis();
    }
}
