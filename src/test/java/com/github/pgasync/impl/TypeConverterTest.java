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
import java.time.*;
import java.util.List;
import java.util.UUID;

import static java.util.Collections.singletonList;
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
        assertNull(dbr.query("select NULL").at(0).getString(0));
    }

    @Test
    public void shouldConvertUnspecifiedToString() {
        assertEquals("test", dbr.query("select 'test'").at(0).getString(0));
    }

    @Test
    public void shouldConvertTextToString() {
        assertEquals("test", dbr.query("select 'test'::TEXT").at(0).getString(0));
    }

    @Test
    public void shouldConvertVarcharToString() {
        assertEquals("test", dbr.query("select 'test'::VARCHAR").at(0).getString(0));
    }

    @Test
    public void shouldConvertCharToString() {
        assertEquals("test ", dbr.query("select 'test'::CHAR(5)").at(0).getString(0));
    }

    @Test
    public void shouldConvertSingleCharToString() {
        assertEquals("X", dbr.query("select 'X'::CHAR AS single").at(0).getString("single"));
    }

    @Test
    public void shouldConvertNullToLong() {
        assertNull(dbr.query("select NULL").at(0).getLong(0));
    }

    @Test
    public void shouldConvertInt8ToLong() {
        assertEquals(5000L, dbr.query("select 5000::INT8").at(0).getLong(0).longValue());
    }

    @Test
    public void shouldConvertInt4ToLong() {
        assertEquals(4000L, dbr.query("select 4000::INT4 AS R").at(0).getLong("R").longValue());
    }

    @Test
    public void shouldConvertInt2ToLong() {
        assertEquals(4000L, dbr.query("select 4000::INT2").at(0).getLong(0).longValue());
    }

    @Test
    public void shouldConvertInt4ToInteger() {
        assertEquals(5000, dbr.query("select 5000::INT4 AS I").at(0).getInt("I").intValue());
    }

    @Test
    public void shouldConvertInt2ToInteger() {
        assertEquals(4000, dbr.query("select 4000::INT2").at(0).getInt(0).intValue());
    }

    @Test
    public void shouldConvertInt2ToShort() {
        assertEquals(3000, dbr.query("select 3000::INT2").at(0).getShort(0).shortValue());
    }

    @Test
    public void shouldConvertInt2ToShortWithName() {
        assertEquals(128, dbr.query("select 128::INT2 AS S").at(0).getShort("S").shortValue());
    }

    @Test
    public void shouldConvertCharToByte() {
        assertEquals(65, dbr.query("select 65::INT2").at(0).getByte(0).byteValue());
    }

    @Test
    public void shouldConvertCharToByteWithName() {
        assertEquals(65, dbr.query("select 65::INT2 as C").at(0).getByte("c").byteValue());
    }

    @Test
    public void shouldConvertInt8ToBigInteger() {
        assertEquals(new BigInteger("9223372036854775807"), dbr.query("select 9223372036854775807::INT8").at(0)
                .getBigInteger(0));
    }

    @Test
    public void shouldConvertInt4ToBigInteger() {
        assertEquals(new BigInteger("1000"), dbr.query("select 1000::INT4 as num").at(0).getBigInteger("num"));
    }

    @Test
    public void shouldConvertFloat8ToBigDecimal() {
        assertEquals(new BigDecimal("123.56"), dbr.query("select 123.56::FLOAT8").at(0).getBigDecimal(0));
    }

    @Test
    public void shouldConvertFloat4ToBigDecimal() {
        assertEquals(new BigDecimal("789.01"), dbr.query("select 789.01::FLOAT4 as sum").at(0).getBigDecimal("sum"));
    }

    @Test
    public void shouldConvertNumericToBigDecimal() {
        assertEquals(new BigDecimal("1223423.01"), dbr.query("select 1223423.01::NUMERIC as sum").at(0).getBigDecimal("sum"));
    }

    @Test
    public void shouldConvertFloat4ToDouble() {
        assertEquals((Double) 1223420.0, dbr.query("select 1223420.0::FLOAT4 as sum").at(0).getDouble("sum"));
    }

    @Test
    public void shouldConvertNumericToDouble() {
        assertEquals((Double) 1223423.01, dbr.query("select 1223423.01::NUMERIC as sum").at(0).getDouble("sum"));
    }

    @Test
    public void shouldConvertDateToDate() {
        assertEquals(Date.valueOf(LocalDate.parse("2014-01-31")),
                dbr.query("select '2014-01-31'::DATE").at(0).getDate(0));
    }

    @Test
    public void shouldConvertDateToDateWithName() {
        assertEquals(Date.valueOf(LocalDate.parse("2014-02-21")), dbr.query("select '2014-02-21'::DATE as D").at(0)
                .getDate("D"));
    }

    @Test
    public void shouldConvertTimeToTime() {
        assertEquals(Time.valueOf(LocalTime.parse("10:15:31.123")), dbr.query("select '10:15:31.123'::TIME").at(0)
                .getTime(0));
    }

    @Test
    public void shouldConvertZonedTimeToTime() {
        assertEquals(Time.valueOf(OffsetTime.parse("23:59:59.999Z").toLocalTime()), dbr.query("select '23:59:59.999Z'::TIMETZ as zoned")
                .at(0).getTime("zoned"));
    }

    @Test
    public void shouldConvertTimestampToTimestamp() {
        assertEquals(Timestamp.valueOf(LocalDateTime.parse("2014-02-21T23:59:59.999")),
                dbr.query("select '2014-02-21 23:59:59.999'::TIMESTAMP as ts").at(0).getTimestamp("ts"));
    }

    @Test
    public void shouldConvertTimestampWithShortMillisToTimestamp() {
        assertEquals(Timestamp.valueOf(LocalDateTime.parse("2014-02-21T23:59:59.990")),
                dbr.query("select '2014-02-21 23:59:59.99'::TIMESTAMP as ts").at(0).getTimestamp("ts"));
    }

    @Test
    public void shouldConvertTimestampWithNoMillisToTimestamp() {
        assertEquals(Timestamp.valueOf(LocalDateTime.parse("2014-02-21T23:59:59")),
                dbr.query("select '2014-02-21 23:59:59'::TIMESTAMP as ts").at(0).getTimestamp("ts"));
    }

    @Test
    public void shouldConvertZonedTimestampToTimestamp() {
        assertEquals(Timestamp.from(Instant.from(ZonedDateTime.parse("2014-02-21T23:59:59.999Z"))),
                dbr.query("select '2014-02-21 23:59:59.999Z'::TIMESTAMPTZ as ts").at(0).getTimestamp("ts"));
    }

    @Test
    public void shouldConvertZonedTimestampWithNanosToTimestamp() {
        assertEquals(Timestamp.valueOf("2014-02-21 23:59:59.000999"),
                dbr.query("select '2014-02-21 23:59:59.000999'::TIMESTAMPTZ as ts").at(0).getTimestamp("ts"));
    }

    @Test
    public void shouldConvertByteAToBytes() {
        assertArrayEquals(new byte[]{0x41, 0x41}, dbr.query("select '\\x4141'::BYTEA").at(0).getBytes(0));
    }

    @Test
    public void shouldConvertByteAToBytesWithName() {
        assertArrayEquals(new byte[]{0x41, 0x41}, dbr.query("select $1::BYTEA as bytes", List.of("AA")).at(0)
                .getBytes("bytes"));
    }

    @Test
    public void shouldConvertBoolean() {
        assertTrue(dbr.query("select $1::BOOL as b", List.of(true)).at(0).getBoolean("b"));
        assertFalse(dbr.query("select $1::BOOL as b", List.of(false)).at(0).getBoolean(0));
        assertNull(dbr.query("select $1::BOOL as b", List.of(new Object[]{null})).at(0).getBoolean("b"));
        assertArrayEquals(new Boolean[]{ true, false}, dbr.query("select '{true,false}'::BOOL[]").at(0).getArray(0, Boolean[].class));
    }

    @Test
    public void shouldConvertUUID() {
        UUID uuid = UUID.randomUUID();
        PgRow row = (PgRow) dbr.query("select $1::UUID as uuid", singletonList(uuid)).at(0);
        assertEquals(uuid, row.get("uuid"));
    }

}
