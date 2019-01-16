package com.pgasync.impl;

import com.github.pgasync.PgRow;
import com.pgasync.Row;
import org.junit.*;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ArrayConversionsTest {
    @ClassRule
    public static DatabaseRule dbr = new DatabaseRule();


    @BeforeClass
    public static void create() {
        drop();
        dbr.query("CREATE TABLE CA_TEST (" +
                "TEXTA TEXT[], SHORTA INT2[], INTA INT4[], LONGA INT8[], FLOATA FLOAT4[], TIMESTAMPA TIMESTAMP[], BYTEAA BYTEA[])");
    }

    @AfterClass
    public static void drop() {
        dbr.query("DROP TABLE IF EXISTS CA_TEST");
    }

    @After
    public void empty() {
        dbr.query("DELETE FROM CA_TEST");
    }

    private Row getRow() {
        return dbr.query("SELECT * FROM CA_TEST").at(0);
    }

    @Test
    public void selectShorts() {
        dbr.query("INSERT INTO CA_TEST (SHORTA) VALUES ('{0, 1, 2, null, 4}')");

        assertArrayEquals(
                new Short[]{0, 1, 2, null, 4},
                getRow().getArray("shorta", Short[].class));
    }

    @Test
    public void selectInts() {
        dbr.query("INSERT INTO CA_TEST (INTA) VALUES ('{0, null, 2, 3}')");

        assertArrayEquals(
                new Integer[]{0, null, 2, 3},
                getRow().getArray("inta", Integer[].class));
    }

    @Test
    public void selectLongs() {
        dbr.query("INSERT INTO CA_TEST (LONGA) VALUES ('{-1, null, 1, 2, 3}')");

        assertArrayEquals(
                new Long[]{-1L, null, 1L, 2L, 3L},
                getRow().getArray("longa", Long[].class));
    }

    @Test
    public void selectIntsMulti() {
        dbr.query("INSERT INTO CA_TEST (INTA) VALUES ('{{{0}, {1}}, {{2}, {3}}}')");

        assertArrayEquals(
                new Integer[][][]{
                        new Integer[][]{new Integer[]{0}, new Integer[]{1}},
                        new Integer[][]{new Integer[]{2}, new Integer[]{3}}},
                getRow().getArray("inta", Integer[][].class));
    }

    @Test
    public void selectText() {
        dbr.query("INSERT INTO CA_TEST (TEXTA) VALUES ('{foo, bar, \"{foo, bar}\"}')");

        assertArrayEquals(
                new String[]{"foo", "bar", "{foo, bar}"},
                getRow().getArray("texta", String[].class));
    }

    @Test
    public void selectTextMulti() {
        dbr.query("INSERT INTO CA_TEST (TEXTA) VALUES (" +
                "'{{f, o, null}, {b, null, r}, {null, a, z}}')");

        assertArrayEquals(
                new String[][]{
                        new String[]{"f", "o", null},
                        new String[]{"b", null, "r"},
                        new String[]{null, "a", "z"}},
                getRow().getArray("texta", String[][].class));
    }

    @Test
    public void selectFloat() {
        dbr.query("INSERT INTO CA_TEST (FLOATA) VALUES ('{177.7, 0, null, -2.012}')");

        assertArrayEquals(
                new BigDecimal[]{
                        new BigDecimal("177.7"),
                        new BigDecimal("0"),
                        null,
                        new BigDecimal("-2.012")
                },
                getRow().getArray("floata", BigDecimal[].class));
    }

    @Test
    public void selectTimestamp() {
        dbr.query("INSERT INTO CA_TEST (TIMESTAMPA) VALUES ('"
                + "{1999-05-16 00:00:00.591, 1970-02-04 01:02:33.01, null}')");

        assertArrayEquals(
                new Timestamp[]{
                        Timestamp.valueOf(LocalDateTime.parse("1999-05-16T00:00:00.591")),
                        Timestamp.valueOf(LocalDateTime.parse("1970-02-04T01:02:33.01")),
                        null
                },
                getRow().getArray("timestampa", Timestamp[].class));
    }

    @Test
    public void selectNull() {
        dbr.query("INSERT INTO CA_TEST (TEXTA) VALUES (NULL);");

        assertArrayEquals(null, getRow().getArray("texta", String[].class));
    }

    @Test
    public void selectNestedInt() {
        Integer[][] a = new Integer[][]{
                new Integer[]{1, 2, 3},
                new Integer[]{4, 5, 6}
        };
        dbr.query("INSERT INTO CA_TEST (INTA) VALUES ($1)", List.of(new Object[]{a}));
        assertArrayEquals(
                a,
                dbr.query(
                        "SELECT INTA FROM CA_TEST WHERE INTA = $1",
                        List.of(new Object[]{a})).at(0).getArray("inta", Integer[].class));
    }

    @Test
    public void selectUTF8() {
        String[] a = new String[]{"U&\"d\\0061t\\+000061\"", "d\u0061t\u0061"};
        dbr.query("INSERT INTO CA_TEST (TEXTA) VALUES ($1)", List.of(new Object[]{a}));
        assertArrayEquals(
                a,
                dbr.query(
                        "SELECT TEXTA FROM CA_TEST WHERE TEXTA = $1",
                        List.of(new Object[]{a})).at(0).getArray("texta", String[].class));
    }

    @Test
    public void selectUnboxed() {
        short[][] a = new short[][]{new short[]{0, 1}, new short[]{1, 0}};
        dbr.query("INSERT INTO CA_TEST (INTA) VALUES ($1)", List.of(new Object[]{a}));
        assertEquals(
                1,
                dbr.query(
                        "SELECT INTA FROM CA_TEST WHERE INTA = $1",
                        List.of(new Object[]{a})).size());
    }

    @Test
    public void implicitGet() {
        dbr.query("INSERT INTO CA_TEST (INTA) VALUES ('{1, 2, 3}')");
        PgRow row = (PgRow) dbr.query("SELECT * FROM CA_TEST").at(0);
        assertArrayEquals(new Integer[]{1, 2, 3}, (Object[]) row.get("inta"));
    }

    @Test
    public void shouldRoundTripTimestamp() {
        List<Timestamp[]> params = new ArrayList<>(1);
        params.add(new Timestamp[]{new Timestamp(12345679), new Timestamp(12345678)});
        dbr.query("INSERT INTO CA_TEST (TIMESTAMPA) VALUES ($1)", params);
        Row row = dbr.query("SELECT TIMESTAMPA FROM CA_TEST WHERE TIMESTAMPA = $1", params).at(0);
        assertArrayEquals(params.get(0), row.getArray(0, Timestamp[].class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowPrimitiveArrays() {
        dbr.query("INSERT INTO CA_TEST (LONGA) VALUES ('{-1, null, 1, 2, 3}')");
        getRow().getArray("longa", long[].class);
    }

    @Test
    public void shouldAllowPrimitiveArrayParameters() {
        long[] input = {1L, 2L, 3L};
        dbr.query("INSERT INTO CA_TEST (LONGA) VALUES ($1)", List.of(input));
        Long[] output = getRow().getArray("longa", Long[].class);
        for (int i = 0; i < input.length; i++) {
            assertEquals(input[i], output[i].longValue());
        }
    }

    @Test
    public void shouldParseUnquotedStringsCorrectly() {
        String[] values = new String[]{"NotNull", "NULLA", "string", null};
        dbr.query("INSERT INTO CA_TEST (TEXTA) VALUES($1)", Collections.singletonList(values));
        Row row = dbr.query("SELECT * FROM CA_TEST").at(0);
        assertArrayEquals(values, row.getArray("texta", String[].class));
    }

    @Test
    public void shouldParseNullTextCorrectly() {
        String[] values = new String[]{"NULL", null, "string"};
        dbr.query("INSERT INTO CA_TEST (TEXTA) VALUES($1)", Collections.singletonList(values));
        Row row = dbr.query("SELECT * FROM CA_TEST").at(0);
        assertArrayEquals(values, row.getArray("texta", String[].class));
    }

    @Test
    public void shouldBindArrayOfByteA() {
        byte[][] bb = new byte[3][];
        bb[0] = "blob 0 content".getBytes(StandardCharsets.UTF_8); // UTF-8 is hard coded here only because the ascii compatible data
        bb[1] = "blob 1 content".getBytes(StandardCharsets.UTF_8); // UTF-8 is hard coded here only because the ascii compatible data
        bb[2] = "blob 2 content".getBytes(StandardCharsets.UTF_8); // UTF-8 is hard coded here only because the ascii compatible data
        dbr.query("INSERT INTO CA_TEST(BYTEAA) VALUES ($1)", Collections.singletonList(bb));
        byte[][] readbb = dbr.query("SELECT BYTEAA FROM CA_TEST WHERE BYTEAA = $1", Collections.singletonList(bb)).at(0).getArray(0, byte[][].class);
        assertEquals(bb.length, readbb.length);
        assertArrayEquals(bb[0], readbb[0]);
        assertArrayEquals(bb[1], readbb[1]);
        assertArrayEquals(bb[2], readbb[2]);
    }

}
