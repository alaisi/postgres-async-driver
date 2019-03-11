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

import com.pgasync.ResultSet;
import com.pgasync.Row;
import com.pgasync.SqlException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;

/**
 * Tests for result set row counts.
 *
 * @author Antti Laisi
 */
public class QueryResultTest {

    @ClassRule
    public static DatabaseRule dbr = new DatabaseRule();

    @BeforeClass
    public static void create() {
        drop();
        dbr.query("CREATE TABLE CONN_TEST(ID INT8)");
    }

    @AfterClass
    public static void drop() {
        dbr.query("DROP TABLE IF EXISTS CONN_TEST");
    }

    @Test
    public void shouldReturnResultSetSize() {
        Assert.assertEquals(2, dbr.query("INSERT INTO CONN_TEST (ID) VALUES (1),(2)").affectedRows());
        ResultSet result = dbr.query("SELECT * FROM CONN_TEST WHERE ID <= 2 ORDER BY ID");
        assertEquals(2, result.size());
        Assert.assertEquals("ID", result.getOrderedColumns().iterator().next().getName().toUpperCase());

        Iterator<Row> i = result.iterator();
        assertEquals(1L, i.next().getLong(0).longValue());
        assertEquals(2L, i.next().getLong(0).longValue());
    }

    @Test
    public void shouldReturnDeletedRowsCount() {
        Assert.assertEquals(1, dbr.query("INSERT INTO CONN_TEST (ID) VALUES (3)").affectedRows());
        Assert.assertEquals(1, dbr.query("DELETE FROM CONN_TEST WHERE ID = 3").affectedRows());
    }

    @Test
    public void shouldReturnUpdatedRowsCount() {
        Assert.assertEquals(3, dbr.query("INSERT INTO CONN_TEST (ID) VALUES (9),(10),(11)").affectedRows());
        Assert.assertEquals(3, dbr.query("UPDATE CONN_TEST SET ID = NULL WHERE ID IN (9,10,11)").affectedRows());
    }

    @Test(expected = SqlException.class)
    public void shouldInvokeErrorHandlerOnError() throws Exception {
        try {
            dbr.query("SELECT * FROM not_there");
        } catch (Exception ex) {
            SqlException.ifCause(ex, sqlException -> {
                throw sqlException;
            }, () -> {
                throw ex;
            });
        }
    }

    @Test
    public void shouldStreamResultRows() throws Exception {
        List<Integer> series = dbr.pool().completeQuery("select generate_series(1, 5)")
                .thenApply(rs -> StreamSupport.stream(rs.spliterator(), false)
                        .map(r -> r.getInt(0))
                        .collect(Collectors.toList()))
                .get();

        assertEquals(List.of(1, 2, 3, 4, 5), series);
    }

}
