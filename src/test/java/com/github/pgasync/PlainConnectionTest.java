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

package com.github.pgasync;

import com.pgasync.Connectible;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

/**
 * Tests for plain connection.
 *
 * @author Marat Gainullin
 */
public class PlainConnectionTest {

    @Rule
    public final DatabaseRule dbr = new DatabaseRule();
    private Connectible plain;

    @Before
    public void create() {
        plain = dbr.builder.plain();
        plain.completeScript("" +
                "DROP TABLE IF EXISTS PC_TEST_1;" +
                "DROP TABLE IF EXISTS PC_TEST_2;" +
                "CREATE TABLE PC_TEST_1 (ID VARCHAR(255) PRIMARY KEY);" +
                "CREATE TABLE PC_TEST_2 (ID VARCHAR(255) PRIMARY KEY);"
        ).join();
    }

    @After
    public void drop() {
        plain.completeScript("" +
                "DROP TABLE PC_TEST_1;" +
                "DROP TABLE PC_TEST_2;"
        ).join();
        plain.close().join();
    }

    @Test
    public void shouldRunAllQueries() {
        final int count = 100;
        IntStream.range(0, count)
                .mapToObj(value -> "" + value)
                .forEach(value -> plain.completeQuery("INSERT INTO PC_TEST_1 VALUES($1)", value).join());

        assertEquals(count, dbr.query("SELECT COUNT(*) FROM PC_TEST_1").at(0).getLong(0).longValue());
    }

    @Test
    public void shouldRunScript() {
        final int count = 25;
        IntStream.range(0, count).forEach(value -> plain.completeScript("" +
                "INSERT INTO PC_TEST_2 VALUES('" + value + "');" +
                "INSERT INTO PC_TEST_2 VALUES('_" + value + "');" +
                "INSERT INTO PC_TEST_2 VALUES('__" + value + "');" +
                "INSERT INTO PC_TEST_2 VALUES('___" + value + "');"
        ).join());

        assertEquals(count * 4, dbr.query("SELECT COUNT(*) FROM PC_TEST_2").at(0).getLong(0).longValue());
    }
}
