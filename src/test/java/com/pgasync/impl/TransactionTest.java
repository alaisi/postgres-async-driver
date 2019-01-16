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

package com.pgasync.impl;

import com.pgasync.ResultSet;
import com.pgasync.SqlException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

/**
 * Tests for BEGIN/COMMIT/ROLLBACK.
 *
 * @author Antti Laisi
 */
public class TransactionTest {

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
        dbr.db().begin()
                .thenApply(transaction -> transaction.completeQuery("SELECT 1")
                        .thenApply(result -> {
                            Assert.assertEquals(1L, result.at(0).getLong(0).longValue());
                            return transaction.commit();
                        })
                        .thenCompose(Function.identity())
                        .handle((v, th) -> transaction.getConnection().close()
                                .thenAccept(_v -> {
                                    if (th != null) {
                                        throw new RuntimeException(th);
                                    }
                                })
                        )
                        .thenCompose(Function.identity())
                )
                .thenCompose(Function.identity())
                .get(5, TimeUnit.SECONDS);
    }

    @Test
    public void shouldCommitInsertInTransaction() throws Exception {
        dbr.db().begin()
                .thenApply(transaction ->
                        transaction.completeQuery("INSERT INTO TX_TEST(ID) VALUES(10)")
                                .thenApply(result -> {
                                    Assert.assertEquals(1, result.affectedRows());
                                    return transaction.commit();
                                })
                                .thenCompose(Function.identity())
                                .handle((v, th) -> transaction.getConnection().close()
                                        .thenAccept(_v -> {
                                            if (th != null) {
                                                throw new RuntimeException(th);
                                            }
                                        })
                                )
                                .thenCompose(Function.identity())
                )
                .thenCompose(Function.identity())
                .get(5, TimeUnit.SECONDS);

        Assert.assertEquals(10L, dbr.query("SELECT ID FROM TX_TEST WHERE ID = 10").at(0).getLong(0).longValue());
    }

    @Test
    public void shouldCommitParameterizedInsertInTransaction() throws Exception {
        // Ref: https://github.com/alaisi/postgres-async-driver/issues/34
        long id = dbr.db().begin()
                .thenApply(transaction ->
                        transaction.completeQuery("INSERT INTO TX_TEST (ID) VALUES ($1) RETURNING ID", 35)
                                .thenApply(rs -> rs.at(0))
                                .thenApply(row -> {
                                    Long value = row.getLong(0);
                                    return transaction.commit().thenApply(v -> value);
                                })
                                .thenCompose(Function.identity())
                                .handle((_id, th) -> transaction.getConnection().close()
                                        .thenApply(v -> {
                                            if (th == null) {
                                                return _id;
                                            } else {
                                                throw new RuntimeException(th);
                                            }
                                        }))
                                .thenCompose(Function.identity())
                )
                .thenCompose(Function.identity())
                .get(5, TimeUnit.SECONDS);
        assertEquals(35L, id);
    }

    @Test
    public void shouldRollbackTransaction() throws Exception {
        dbr.db().begin()
                .thenApply(transaction ->
                        transaction.completeQuery("INSERT INTO TX_TEST(ID) VALUES(9)")
                                .thenApply(result -> {
                                    Assert.assertEquals(1, result.affectedRows());
                                    return transaction.rollback();
                                })
                                .thenCompose(Function.identity())
                                .handle((v, th) -> transaction.getConnection().close()
                                        .thenAccept(_v -> {
                                            if (th != null) {
                                                throw new RuntimeException(th);
                                            }
                                        })
                                )
                                .thenCompose(Function.identity())
                )
                .thenCompose(Function.identity())
                .get(5, TimeUnit.SECONDS);

        Assert.assertEquals(0L, dbr.query("SELECT ID FROM TX_TEST WHERE ID = 9").size());
    }


    @Test(expected = SqlException.class)
    public void shouldRollbackTransactionOnBackendError() throws Exception {
        try {
            dbr.db().begin()
                    .thenApply(transaction ->
                            transaction.completeQuery("INSERT INTO TX_TEST(ID) VALUES(11)")
                                    .thenApply(result -> {
                                        Assert.assertEquals(1, result.affectedRows());
                                        return transaction.completeQuery("INSERT INTO TX_TEST(ID) VALUES(11)");
                                    })
                                    .thenCompose(Function.identity())
                                    .handle((v, th) -> transaction.getConnection().close()
                                            .thenAccept(_v -> {
                                                if (th != null) {
                                                    throw new RuntimeException(th);
                                                }
                                            })
                                    )
                                    .thenCompose(Function.identity())
                    )
                    .thenCompose(Function.identity())
                    .get(5, TimeUnit.SECONDS);
        } catch (Exception ex) {
            SqlException.ifCause(ex, sqlException -> {
                Assert.assertEquals(0, dbr.query("SELECT ID FROM TX_TEST WHERE ID = 11").size());
                throw sqlException;
            }, () -> {
                throw ex;
            });
        }
    }

    @Test
    public void shouldRollbackTransactionAfterBackendError() throws Exception {
        dbr.db().begin()
                .thenApply(transaction ->
                        transaction.completeQuery("INSERT INTO TX_TEST(ID) VALUES(22)")
                                .thenApply(result -> {
                                    Assert.assertEquals(1, result.affectedRows());
                                    return transaction.completeQuery("INSERT INTO TX_TEST(ID) VALUES(22)")
                                            .thenApply(rs -> CompletableFuture.<ResultSet>failedFuture(new IllegalStateException("The transaction should fail")))
                                            .exceptionally(th -> transaction.completeQuery("SELECT 1"))
                                            .thenCompose(Function.identity());
                                })
                                .thenCompose(Function.identity())
                                .handle((rs, th) -> transaction.getConnection().close()
                                        .thenAccept(v -> {
                                            if (th != null) {
                                                throw new RuntimeException(th);
                                            }
                                        })
                                )
                                .thenCompose(Function.identity())
                )
                .thenCompose(Function.identity())
                .get(5, TimeUnit.SECONDS);
        Assert.assertEquals(0, dbr.query("SELECT ID FROM TX_TEST WHERE ID = 22").size());
    }

    @Test
    public void shouldSupportNestedTransactions() throws Exception {
        dbr.db().begin()
                .thenApply(transaction ->
                        transaction.begin()
                                .thenApply(nested ->
                                        nested.completeQuery("INSERT INTO TX_TEST(ID) VALUES(19)")
                                                .thenApply(result -> {
                                                    Assert.assertEquals(1, result.affectedRows());
                                                    return nested.commit();
                                                })
                                                .thenCompose(Function.identity())
                                                .thenApply(v -> transaction.commit())
                                                .thenCompose(Function.identity())
                                )
                                .thenCompose(Function.identity())
                                .handle((v, th) -> transaction.getConnection().close()
                                        .thenAccept(_v -> {
                                            if (th != null) {
                                                throw new RuntimeException(th);
                                            }
                                        })
                                )
                                .thenCompose(Function.identity())
                )
                .thenCompose(Function.identity())
                .get(5, TimeUnit.SECONDS);
        Assert.assertEquals(1L, dbr.query("SELECT ID FROM TX_TEST WHERE ID = 19").size());
    }

    @Test
    public void shouldRollbackNestedTransaction() throws Exception {
        dbr.db().begin()
                .thenApply(transaction ->
                        transaction.completeQuery("INSERT INTO TX_TEST(ID) VALUES(24)")
                                .thenApply(result -> {
                                    Assert.assertEquals(1, result.affectedRows());
                                    return transaction.begin()
                                            .thenApply(nested ->
                                                    nested.completeQuery("INSERT INTO TX_TEST(ID) VALUES(23)")
                                                            .thenApply(res2 -> {
                                                                Assert.assertEquals(1, res2.affectedRows());
                                                                return nested.rollback();
                                                            })
                                                            .thenCompose(Function.identity())
                                            )
                                            .thenCompose(Function.identity())
                                            .thenApply(v -> transaction.commit())
                                            .thenCompose(Function.identity());
                                })
                                .thenCompose(Function.identity())
                                .handle((v, th) -> transaction.getConnection().close()
                                        .thenAccept(_v -> {
                                            if (th != null) {
                                                throw new RuntimeException(th);
                                            }
                                        })
                                )
                                .thenCompose(Function.identity())
                )
                .thenCompose(Function.identity())
                .get(5, TimeUnit.SECONDS);
        Assert.assertEquals(1L, dbr.query("SELECT ID FROM TX_TEST WHERE ID = 24").size());
        Assert.assertEquals(0L, dbr.query("SELECT ID FROM TX_TEST WHERE ID = 23").size());
    }

    @Test
    public void shouldRollbackNestedTransactionOnBackendError() throws Exception {
        dbr.db().begin()
                .thenApply(transaction ->
                        transaction.completeQuery("INSERT INTO TX_TEST(ID) VALUES(25)")
                                .thenApply(result -> {
                                    Assert.assertEquals(1, result.affectedRows());
                                    return transaction.begin()
                                            .thenApply(nested ->
                                                    nested.completeQuery("INSERT INTO TX_TEST(ID) VALUES(26)")
                                                            .thenAccept(res2 -> Assert.assertEquals(1, res2.affectedRows()))
                                                            .thenApply(v -> nested.completeQuery("INSERT INTO TX_TEST(ID) VALUES(26)"))
                                                            .thenCompose(Function.identity())
                                                            .thenApply(rs -> CompletableFuture.<Void>failedFuture(new IllegalStateException("The query should fail")))
                                                            .exceptionally(th -> transaction.commit())
                                                            .thenCompose(Function.identity())
                                            )
                                            .thenCompose(Function.identity());
                                })
                                .thenCompose(Function.identity())
                                .handle((v, th) -> transaction.getConnection().close()
                                        .thenAccept(_v -> {
                                            if (th != null) {
                                                throw new RuntimeException(th);
                                            }
                                        })
                                )
                                .thenCompose(Function.identity())
                )
                .thenCompose(Function.identity())
                .get(5, TimeUnit.SECONDS);
        Assert.assertEquals(1L, dbr.query("SELECT ID FROM TX_TEST WHERE ID = 25").size());
        Assert.assertEquals(0L, dbr.query("SELECT ID FROM TX_TEST WHERE ID = 26").size());
    }
}
