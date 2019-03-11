package com.github.pgasync;

import com.pgasync.Converter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Antti Laisi
 */
public class CustomConverterTest {

    static class Json {
        final String json;

        Json(String json) {
            this.json = json;
        }
    }

    static class JsonConverter implements Converter<Json> {
        @Override
        public Class<Json> type() {
            return Json.class;
        }

        @Override
        public String from(Json o) {
            return o.json;
        }

        @Override
        public Json to(Oid oid, String value) {
            return new Json(value);
        }
    }

    @ClassRule
    public static DatabaseRule dbr = new DatabaseRule(
            DatabaseRule.createPoolBuilder(1)
                    .converters(new JsonConverter())
    );

    @BeforeClass
    public static void create() {
        drop();
        dbr.query("CREATE TABLE CC_TEST (ID BIGINT, JS JSON)");
    }

    @AfterClass
    public static void drop() {
        dbr.query("DROP TABLE IF EXISTS CC_TEST");
    }

    @Test
    public void shouldConvertColumnDataToType() {
        dbr.query("INSERT INTO CC_TEST VALUES (1, $1)", List.of(new Json("{\"a\": 1}")));
        assertEquals("{\"a\": 1}", dbr.query("SELECT * FROM CC_TEST WHERE ID = 1").at(0).get("js", Json.class).json);
    }

    @Test
    public void shouldConvertParameter() {
        dbr.query("INSERT INTO CC_TEST VALUES (2, $1)", List.of(new Json("{\"b\": 2}")));
        assertEquals("{\"b\": 2}", dbr.query("SELECT * FROM CC_TEST WHERE ID = 2").at(0).get("js", Json.class).json);
    }

}
