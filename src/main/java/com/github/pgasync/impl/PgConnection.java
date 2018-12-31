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

import static com.github.pgasync.impl.message.RowDescription.ColumnDescription;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.pgasync.Connection;
import com.github.pgasync.ResultSet;
import com.github.pgasync.Row;
import com.github.pgasync.Transaction;
import com.github.pgasync.impl.conversion.DataConverter;
import com.github.pgasync.impl.message.Authentication;
import com.github.pgasync.impl.message.Bind;
import com.github.pgasync.impl.message.CommandComplete;
import com.github.pgasync.impl.message.DataRow;
import com.github.pgasync.impl.message.ExtendedQuery;
import com.github.pgasync.impl.message.Message;
import com.github.pgasync.impl.message.Parse;
import com.github.pgasync.impl.message.PasswordMessage;
import com.github.pgasync.impl.message.Query;
import com.github.pgasync.impl.message.ReadyForQuery;
import com.github.pgasync.impl.message.RowDescription;
import com.github.pgasync.impl.message.StartupMessage;

/**
 * A connection to PostgreSQL backed. The postmaster forks a backend process for
 * each connection. A connection can process only a single queryRows at a time.
 *
 * @author Antti Laisi
 */
public class PgConnection implements Connection {

    final PgProtocolStream stream;
    final DataConverter dataConverter;

    public PgConnection(PgProtocolStream stream, DataConverter dataConverter) {
        this.stream = stream;
        this.dataConverter = dataConverter;
    }

    CompletableFuture<Connection> connect(String username, String password, String database) {
        return stream.connect(new StartupMessage(username, database))
                .thenApply(message -> authenticate(username, password, message))
                .thenCompose(Function.identity())
                .thenApply(message -> {
                    if (message == ReadyForQuery.INSTANCE) {
                        return this;
                    } else {
                        throw new IllegalStateException("Unexpected Postgres message detected");
                    }
                });
    }

    CompletableFuture<? extends Message> authenticate(String username, String password, Message message) {
        return message instanceof Authentication && !((Authentication) message).isAuthenticationOk()
                ? stream.authenticate(new PasswordMessage(username, password, ((Authentication) message).getMd5Salt()))
                : CompletableFuture.completedFuture(message);
    }

    boolean isConnected() {
        return stream.isConnected();
    }

    @Override
    public CompletableFuture<ResultSet> querySet(String sql, Object... params) {
        return sendQuery(sql, params)
                .thenApply(toResultSet(dataConverter));
    }

    @Override
    public CompletableFuture<Row> queryRows(String sql, Object... params) {
        return sendQuery(sql, params)
                .thenApply(toRow(dataConverter));
    }

    @Override
    public CompletableFuture<Transaction> begin() {
        return querySet("BEGIN")
                .thenApply(rs -> new PgConnectionTransaction());
    }

    /*
        @Override
        public CompletableFuture<String> listen(String channel) {
            // TODO: wait for commit before sending unlisten as otherwise it can be rolled back
            return querySet("LISTEN " + channel)
                    .<String>lift(subscriber -> Subscribers.create(rs -> stream.listen(channel)
                                    .subscribe(subscriber),
                            subscriber::onError))
                    .doOnUnsubscribe(() -> querySet("UNLISTEN " + channel).subscribe(rs -> {
                    }));
        }
    */
    @Override
    public void close() {
        stream.close()
                .exceptionally(th -> {
                    Logger.getLogger(PgConnection.class.getName()).log(Level.SEVERE, null, th);
                    return null;
                });
    }

    private CompletableFuture<Message> sendQuery(String sql, Object[] params) {
        return params == null || params.length == 0
                ? stream.send(new Query(sql))
                : stream.send(new Parse(sql), new Bind(dataConverter.fromParameters(params)),
                ExtendedQuery.DESCRIBE,
                ExtendedQuery.EXECUTE,
                ExtendedQuery.CLOSE,
                ExtendedQuery.SYNC);
    }
    static Function<? super Message, Row> toRow(DataConverter dataConverter) {
        return null;
/*
        return (message) -> {
            Map<String, PgColumn> columns;
            if (message instanceof RowDescription) {
                columns = getColumns(((RowDescription) message).getColumns());
            } else if (message instanceof DataRow) {
                return new PgRow((DataRow) message, columns, dataConverter);
            }
        };
        */
    }

    static Function<? super Message, ResultSet> toResultSet(DataConverter dataConverter) {
        return null;
        /*
        return subscriber -> new Subscriber<Message>() {

            Map<String, PgColumn> columns;
            List<Row> rows = new ArrayList<>();
            int updated;

            @Override
            public void onNext(Message message) {
                if (message instanceof RowDescription) {
                    columns = getColumns(((RowDescription) message).getColumns());
                } else if (message instanceof DataRow) {
                    rows.add(new PgRow((DataRow) message, columns, dataConverter));
                } else if (message instanceof CommandComplete) {
                    updated = ((CommandComplete) message).getUpdatedRows();
                } else if (message == ReadyForQuery.INSTANCE) {
                    subscriber.onNext(new PgResultSet(columns, rows, updated));
                }
            }

            @Override
            public void onError(Throwable e) {
                subscriber.onError(e);
            }

            @Override
            public void onCompleted() {
                subscriber.onCompleted();
            }
        };
        */
    }

    static Map<String, PgColumn> getColumns(ColumnDescription[] descriptions) {
        Map<String, PgColumn> columns = new HashMap<>();
        for (int i = 0; i < descriptions.length; i++) {
            columns.put(descriptions[i].getName().toUpperCase(), new PgColumn(i, descriptions[i].getName(), descriptions[i].getType()));
        }
        return columns;
    }

    /**
     * Transaction that rollbacks the tx on backend error and closes the connection on COMMIT/ROLLBACK failure.
     */
    class PgConnectionTransaction implements Transaction {

        @Override
        public CompletableFuture<Transaction> begin() {
            return querySet("SAVEPOINT sp_1")
                    .thenApply(rs -> new PgConnectionNestedTransaction(1));
        }

        @Override
        public CompletableFuture<Void> commit() {
            return PgConnection.this.querySet("COMMIT")
                    .thenAccept(rs -> {
                    })
                    .exceptionally(th -> {
                        // TODO: Add logs
                        stream.close();
                        return null;
                    });
        }

        @Override
        public CompletableFuture<Void> rollback() {
            return PgConnection.this.querySet("ROLLBACK")
                    .thenAccept(rs -> {
                    })
                    .exceptionally(th -> {
                        // TODO: Add logs
                        stream.close();
                        return null;
                    });
        }

        @Override
        public CompletableFuture<Row> queryRows(String sql, Object... params) {
            return PgConnection.this.queryRows(sql, params)
                    .exceptionally(th -> {
                        rollback();
                        throw new IllegalStateException(th);
                    });
        }

        @Override
        public CompletableFuture<ResultSet> querySet(String sql, Object... params) {
            return PgConnection.this.querySet(sql, params)
                    .exceptionally(th -> {
                        rollback();
                        throw new IllegalStateException(th);
                    });
        }
    }

    /**
     * Nested Transaction using savepoints.
     */
    class PgConnectionNestedTransaction extends PgConnectionTransaction {

        final int depth;

        PgConnectionNestedTransaction(int depth) {
            this.depth = depth;
        }

        @Override
        public CompletableFuture<Transaction> begin() {
            return querySet("SAVEPOINT sp_" + (depth + 1))
                    .thenApply(rs -> new PgConnectionNestedTransaction(depth + 1));
        }

        @Override
        public CompletableFuture<Void> commit() {
            return PgConnection.this.querySet("RELEASE SAVEPOINT sp_" + depth)
                    .thenApply(rs -> null);
        }

        @Override
        public CompletableFuture<Void> rollback() {
            return PgConnection.this.querySet("ROLLBACK TO SAVEPOINT sp_" + depth)
                    .thenApply(rs -> null);
        }
    }
}
