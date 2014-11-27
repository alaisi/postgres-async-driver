package com.github.pgasync.impl;

import com.github.pgasync.impl.conversion.ArrayConverter;
import com.github.pgasync.impl.conversion.DataConverter;
import org.junit.*;

import java.math.BigDecimal;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

public class ArrayConverterTest {
    @ClassRule
    public static DatabaseRule dbr = new DatabaseRule(DatabaseRule
        .createPoolBuilder(1)
        .converters(new ArrayConverter(new DataConverter())));

    @BeforeClass
    public static void create() {
        drop();
        dbr.query("CREATE TABLE CA_TEST (TEXTA TEXT[], INTA INT4[], FLOATA FLOAT4[])");
    }

    @AfterClass
    public static void drop() {
        dbr.query("DROP TABLE IF EXISTS CA_TEST");
    }

    protected Object[] getArrayColumn(final String name, final Object[] value) {
        return dbr.query(
            "SELECT * FROM CA_TEST WHERE " + name + " = $1",
            asList(new PgArray(value))
        ).row(0).get(name, PgArray.class).getArray();
    }

    public void runRoundtrip(final String col, final Object[] value) {
        dbr.query(
            "INSERT INTO CA_TEST (" + col + ") VALUES ($1)",
            asList(new PgArray(value)));
        final Object[] result = getArrayColumn(col, value);
        assertArrayEquals(value, result);
    }

    @Test
    public void textRoundtrip() {
        runRoundtrip("TEXTA", new String[]{"three", "blind", "mice"});
    }

    @Test
    public void integerRoundtrip() {
        runRoundtrip("INTA", new Integer[]{-2147483647, 0, 2147483647});
    }

    @Test
    public void decimalRoundtrip() {
        runRoundtrip(
            "FLOATA",
            new BigDecimal[]{
                new BigDecimal("177.7"),
                new BigDecimal("0"),
                new BigDecimal("-2.012")
            });
    }

    @Test
    public void text2dRoundtrip() {
        runRoundtrip(
            "TEXTA",
            new String[][]{
                new String[]{"x,b,c", "y", "\"\\\""},
                new String[]{"a", "z", "t"}
            });
    }

    @Test
    public void nullColumn() {
        dbr.query("INSERT INTO CA_TEST (TEXTA) VALUES (NULL)");
        assertNull(
            dbr.query(
                "SELECT * FROM CA_TEST WHERE TEXTA IS NULL"
            ).row(0).get("TEXTA", PgArray.class));
    }

    @Test
    public void nullIntegerRoundtrip() {
        runRoundtrip(
            "INTA",
            new Integer[][][]{
                new Integer[][]{
                    new Integer[]{ null, 1, 2, null },
                    new Integer[]{ 1, null, 4, null }
                },
                new Integer[][]{
                    new Integer[]{ 12, null, 34, null },
                    new Integer[]{ null, null, null, null }
                }
            });
    }
}