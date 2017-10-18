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
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for BEGIN/COMMIT/ROLLBACK.
 * 
 * @author Antti Laisi
 */
public class TransactionTest {

    final Consumer<Throwable> err = Throwable::printStackTrace;
    final Consumer<ResultSet> fail = result -> new AssertionError("Failure expected").printStackTrace();

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

    @Test
    public void shouldCommitSelectInTransaction() throws Exception {
        CountDownLatch sync = new CountDownLatch(1);

        dbr.db().begin((transaction) ->
                transaction.query("SELECT 1", result -> {
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
                transaction.query("INSERT INTO TX_TEST(ID) VALUES(10)", result -> {
                    assertEquals(1, result.updatedRows());
                    transaction.commit(sync::countDown, err);
                }, err),
            err);

        assertTrue(sync.await(5, TimeUnit.SECONDS));
        assertEquals(10L, dbr.query("SELECT ID FROM TX_TEST WHERE ID = 10").row(0).getLong(0).longValue());
    }

    @Test
    public void shouldCommitParameterizedInsertInTransaction() throws Exception {
        // Ref: https://github.com/alaisi/postgres-async-driver/issues/34
        long id = dbr.db().begin().flatMap(txn ->
            txn.queryRows("INSERT INTO TX_TEST (ID) VALUES ($1) RETURNING ID", "35").last().flatMap(row -> {
                Long value = row.getLong(0);
                return txn.commit().map(v -> value);
            })
        ).toBlocking().single();
        assertEquals(35L, id);
    }

    @Test
    public void shouldRollbackTransaction() throws Exception {
        CountDownLatch sync = new CountDownLatch(1);

        dbr.db().begin((transaction) ->
                transaction.query("INSERT INTO TX_TEST(ID) VALUES(9)", result -> {
                    assertEquals(1, result.updatedRows());
                    transaction.rollback(sync::countDown, err);
                }, err),
            err);

        assertTrue(sync.await(5, TimeUnit.SECONDS));
        assertEquals(0L, dbr.query("SELECT ID FROM TX_TEST WHERE ID = 9").size());
    }


    @Test
    public void shouldRollbackTransactionOnBackendError() throws Exception {
        CountDownLatch sync = new CountDownLatch(1);

        dbr.db().begin(transaction ->
                transaction.query("INSERT INTO TX_TEST(ID) VALUES(11)", result -> {
                    assertEquals(1, result.updatedRows());
                    transaction.query("INSERT INTO TX_TEST(ID) VALUES(11)", fail, t -> sync.countDown());
                }, err),
            err);

        assertTrue(sync.await(5, TimeUnit.SECONDS));
        assertEquals(0, dbr.query("SELECT ID FROM TX_TEST WHERE ID = 11").size());
    }

    @Test
    public void shouldInvalidateTxConnAfterError() throws Exception {
        CountDownLatch sync = new CountDownLatch(1);

        dbr.db().begin((transaction) ->
                transaction.query("INSERT INTO TX_TEST(ID) VALUES(22)", result -> {
                    assertEquals(1, result.updatedRows());
                    transaction.query("INSERT INTO TX_TEST(ID) VALUES(22)", fail, t ->
                            transaction.query("SELECT 1", fail, t1 -> {
                                assertEquals("Transaction is already completed", t1.getMessage());
                                sync.countDown();
                            }));
                }, err),
            err);

        assertTrue(sync.await(5, TimeUnit.SECONDS));
        assertEquals(0, dbr.query("SELECT ID FROM TX_TEST WHERE ID = 22").size());
    }

    @Test
    public void shouldSupportNestedTransactions() throws Exception {
        CountDownLatch sync = new CountDownLatch(1);

        dbr.db().begin((transaction) ->
                transaction.begin((nested) ->
                    nested.query("INSERT INTO TX_TEST(ID) VALUES(19)", result -> {
                        assertEquals(1, result.updatedRows());
                        nested.commit(() -> transaction.commit(sync::countDown, err), err);
                    }, err),
                err),
            err);

        assertTrue(sync.await(5, TimeUnit.SECONDS));
        assertEquals(1L, dbr.query("SELECT ID FROM TX_TEST WHERE ID = 19").size());
    }

    @Test
    public void shouldRollbackNestedTransaction() throws Exception {
        CountDownLatch sync = new CountDownLatch(1);

        dbr.db().begin((transaction) ->
                transaction.query("INSERT INTO TX_TEST(ID) VALUES(24)", result -> {
                    assertEquals(1, result.updatedRows());
                    transaction.begin((nested) ->
                        nested.query("INSERT INTO TX_TEST(ID) VALUES(23)", res2 -> {
                            assertEquals(1, res2.updatedRows());
                            nested.rollback(() -> transaction.commit(sync::countDown, err), err);
                        }, err), err);
                }, err),
            err);

        assertTrue(sync.await(5, TimeUnit.SECONDS));
        assertEquals(1L, dbr.query("SELECT ID FROM TX_TEST WHERE ID = 24").size());
        assertEquals(0L, dbr.query("SELECT ID FROM TX_TEST WHERE ID = 23").size());
    }

    @Test
    public void shouldRollbackNestedTransactionOnBackendError() throws Exception {
        CountDownLatch sync = new CountDownLatch(1);

        dbr.db().begin((transaction) ->
                transaction.query("INSERT INTO TX_TEST(ID) VALUES(25)", result -> {
                    assertEquals(1, result.updatedRows());
                    transaction.begin((nested) ->
                        nested.query("INSERT INTO TX_TEST(ID) VALUES(26)", res2 -> {
                            assertEquals(1, res2.updatedRows());
                            nested.query("INSERT INTO TD_TEST(ID) VALUES(26)",
                                fail, t -> transaction.commit(sync::countDown, err));
                        }, err), err);
                }, err),
            err);

        assertTrue(sync.await(5, TimeUnit.SECONDS));
        assertEquals(1L, dbr.query("SELECT ID FROM TX_TEST WHERE ID = 25").size());
        assertEquals(0L, dbr.query("SELECT ID FROM TX_TEST WHERE ID = 26").size());
    }
}
