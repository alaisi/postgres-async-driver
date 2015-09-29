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

import com.github.pgasync.ResultSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.util.function.Consumer;

import static org.junit.Assert.fail;

/**
 * Tests for BEGIN/COMMIT/ROLLBACK.
 * 
 * @author Antti Laisi
 */
public class TransactionTest {

    final Consumer<Throwable> err = t -> { throw new AssertionError("failed", t); };
    final Consumer<ResultSet> fail = result -> fail();

    @ClassRule
    public static DatabaseRule dbr = new DatabaseRule();

    @BeforeClass
    public static void create() {
        drop();
        dbr.query("CREATE TABLE TX_TEST(ID INT8 PRIMARY KEY)");
    }

    @AfterClass
    public static void drop() {
        dbr.query("DROP TABLE IF EXISTS TX_TEST");
    }
/*
    @Test
    public void shouldCommitSelectInTransaction() throws Exception {
        CountDownLatch sync = new CountDownLatch(1);

        dbr.db().begin((transaction) ->
                transaction.queryRows("SELECT 1", result -> {
                    assertEquals(1L, result.row(0).getLong(0).longValue());
                    transaction.commit(sync::countDown, err);
                }, err),
            err);

        assertTrue(sync.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void shouldCommitInsertInTransaction() throws Exception {
        CountDownLatch sync = new CountDownLatch(1);

        dbr.db().begin((transaction) ->
                transaction.queryRows("INSERT INTO TX_TEST(ID) VALUES(10)", result -> {
                    assertEquals(1, result.updatedRows());
                    transaction.commit(sync::countDown, err);
                }, err),
            err);

        assertTrue(sync.await(5, TimeUnit.SECONDS));
        assertEquals(10L, dbr.queryRows("SELECT ID FROM TX_TEST WHERE ID = 10").row(0).getLong(0).longValue());
    }

    @Test
    public void shouldRollbackTransaction() throws Exception {
        CountDownLatch sync = new CountDownLatch(1);

        dbr.db().begin((transaction) ->
                transaction.queryRows("INSERT INTO TX_TEST(ID) VALUES(9)", result -> {
                    assertEquals(1, result.updatedRows());
                    transaction.rollback(sync::countDown, err);
                }, err),
            err);

        assertTrue(sync.await(5, TimeUnit.SECONDS));
        assertEquals(0L, dbr.queryRows("SELECT ID FROM TX_TEST WHERE ID = 9").size());
    }

    @Test
    public void shouldRollbackTransactionOnBackendError() throws Exception {
        CountDownLatch sync = new CountDownLatch(1);

        dbr.db().begin((transaction) ->
                transaction.queryRows("INSERT INTO TX_TEST(ID) VALUES(11)", result -> {
                    assertEquals(1, result.updatedRows());
                    transaction.queryRows("INSERT INTO TX_TEST(ID) VALUES(11)", fail, t -> sync.countDown());
                }, err),
            err);

        assertTrue(sync.await(5, TimeUnit.SECONDS));
        assertEquals(0, dbr.queryRows("SELECT ID FROM TX_TEST WHERE ID = 11").size());
    }
    
    @Test
    public void shouldInvalidateTxConnAfterError() throws Exception {
        CountDownLatch sync = new CountDownLatch(1);

        dbr.db().begin((transaction) ->
                transaction.queryRows("INSERT INTO TX_TEST(ID) VALUES(22)", result -> {
                    assertEquals(1, result.updatedRows());
                    transaction.queryRows("INSERT INTO TX_TEST(ID) VALUES(22)", fail, t ->
                            transaction.queryRows("SELECT 1", fail, t1 -> {
                                assertEquals("Transaction is rolled back", t1.getMessage());
                                sync.countDown();
                            }));
                }, err),
            err);

        assertTrue(sync.await(5, TimeUnit.SECONDS));
        assertEquals(0, dbr.queryRows("SELECT ID FROM TX_TEST WHERE ID = 22").size());
    }
    */
}
