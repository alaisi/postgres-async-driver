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

import static com.github.pgasync.message.backend.RowDescription.ColumnDescription;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.pgasync.message.backend.DataRow;
import com.pgasync.Connection;
import com.pgasync.Listening;
import com.pgasync.PreparedStatement;
import com.pgasync.ResultSet;
import com.pgasync.Row;
import com.pgasync.Transaction;
import com.github.pgasync.conversion.DataConverter;
import com.github.pgasync.message.backend.Authentication;
import com.github.pgasync.message.frontend.Bind;
import com.github.pgasync.message.frontend.Close;
import com.github.pgasync.message.frontend.Describe;
import com.github.pgasync.message.Message;
import com.github.pgasync.message.frontend.Parse;
import com.github.pgasync.message.frontend.PasswordMessage;
import com.github.pgasync.message.frontend.Query;
import com.github.pgasync.message.frontend.StartupMessage;

/**
 * A connection to Postgres backend. The postmaster forks a backend process for
 * each connection. A connection can process only a single query at a time.
 *
 * @author Antti Laisi
 */
public class PgConnection implements Connection {

    /**
     * Uses named server side prepared statement and named portal.
     */
    public class PgPreparedStatement implements PreparedStatement {

        private final String sname;
        private Columns columns;

        PgPreparedStatement(String sname) {
            this.sname = sname;
        }

        @Override
        public CompletableFuture<ResultSet> query(Object... params) {
            List<Row> rows = new ArrayList<>();
            return fetch((columnsByName, orderedColumns) -> {
            }, rows::add, params)
                    .thenApply(v -> new PgResultSet(columns.byName, List.of(columns.ordered), rows, 0));
        }

        @Override
        public CompletableFuture<Integer> fetch(BiConsumer<Map<String, PgColumn>, PgColumn[]> onColumns, Consumer<Row> processor, Object... params) {
            Bind bind = new Bind(sname, dataConverter.fromParameters(params));
            Consumer<DataRow> rowProcessor = dataRow -> processor.accept(new PgRow(dataRow, columns.byName, columns.ordered, dataConverter));
            if (columns != null) {
                return stream
                        .send(bind, rowProcessor);
            } else {
                return stream
                        .send(bind, Describe.portal(), columnDescriptions -> {
                            columns = calcColumns(columnDescriptions);
                            onColumns.accept(columns.byName, columns.ordered);
                        }, rowProcessor);
            }
        }

        @Override
        public CompletableFuture<Void> close() {
            return stream.send(Close.statement(sname)).thenAccept(closeComplete -> {
            });
        }
    }

    public static class Columns {
        final Map<String, PgColumn> byName;
        final PgColumn[] ordered;

        Columns(Map<String, PgColumn> byName, PgColumn[] ordered) {
            this.byName = byName;
            this.ordered = ordered;
        }
    }

    private static class NameSequence {

        private long counter;
        private String prefix;

        NameSequence(final String prefix) {
            this.prefix = prefix;
        }

        private String next() {
            if (counter == Long.MAX_VALUE) {
                counter = 0;
                prefix = "_" + prefix;
            }
            return prefix + ++counter;
        }
    }

    private static final NameSequence preparedStatementNames = new NameSequence("s-");

    private final PgProtocolStream stream;
    private final DataConverter dataConverter;
    private final Charset encoding;

    PgConnection(PgProtocolStream stream, DataConverter dataConverter, Charset encoding) {
        this.stream = stream;
        this.dataConverter = dataConverter;
        this.encoding = encoding;
    }

    CompletableFuture<Connection> connect(String username, String password, String database) {
        return stream.connect(new StartupMessage(username, database))
                .thenApply(authentication -> authenticate(username, password, authentication))
                .thenCompose(Function.identity())
                .thenApply(authenticationOk -> PgConnection.this);
    }

    private CompletableFuture<? extends Message> authenticate(String username, String password, Message message) {
        return message instanceof Authentication && !((Authentication) message).isAuthenticationOk()
                ? stream.authenticate(new PasswordMessage(username, password, ((Authentication) message).getMd5Salt(), encoding))
                : CompletableFuture.completedFuture(message);
    }

    boolean isConnected() {
        return stream.isConnected();
    }

    @Override
    public CompletableFuture<PreparedStatement> prepareStatement(String sql, Oid... parametersTypes) {
        return preparedStatementOf(sql, parametersTypes).thenApply(pgStmt -> pgStmt);
    }

    CompletableFuture<PgPreparedStatement> preparedStatementOf(String sql, Oid... parametersTypes) {
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("'sql' shouldn't be null or empty or blank string");
        }
        if (parametersTypes == null) {
            throw new IllegalArgumentException("'parametersTypes' shouldn't be null, atr least it should be empty");
        }
        String statementName = preparedStatementNames.next();
        return stream
                .send(new Parse(sql, statementName, parametersTypes))
                .thenApply(parseComplete -> new PgPreparedStatement(statementName));
    }

    @Override
    public CompletableFuture<Void> script(BiConsumer<Map<String, PgColumn>, PgColumn[]> onColumns, Consumer<Row> onRow, Consumer<Integer> onAffected, String sql) {
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("'sql' shouldn't be null or empty or blank string");
        }
        AtomicReference<Columns> columnsRef = new AtomicReference<>();
        return stream.send(
                new Query(sql),
                columnDescriptions -> {
                    Columns columns = calcColumns(columnDescriptions);
                    columnsRef.set(columns);
                    onColumns.accept(columns.byName, columns.ordered);
                },
                message -> onRow.accept(new PgRow(message, columnsRef.get().byName, columnsRef.get().ordered, dataConverter)),
                message -> onAffected.accept(message.getAffectedRows())
        );
    }

    @Override
    public CompletableFuture<Integer> query(BiConsumer<Map<String, PgColumn>, PgColumn[]> onColumns, Consumer<Row> onRow, String sql, Object... params) {
        return prepareStatement(sql, dataConverter.assumeTypes(params))
                .thenApply(ps -> ps.fetch(onColumns, onRow, params)
                        .handle((affected, th) -> ps.close()
                                .thenApply(v -> {
                                    if (th != null)
                                        throw new RuntimeException(th);
                                    else
                                        return affected;
                                })
                        )
                        .thenCompose(Function.identity())
                )
                .thenCompose(Function.identity());
    }

    @Override
    public CompletableFuture<Transaction> begin() {
        return completeScript("BEGIN")
                .thenApply(rs -> new PgConnectionTransaction());
    }

    public CompletableFuture<Listening> subscribe(String channel, Consumer<String> onNotification) {
        // TODO: wait for commit before sending unlisten as otherwise it can be rolled back
        return completeScript("LISTEN " + channel)
                .thenApply(results -> {
                    Runnable unsubscribe = stream.subscribe(channel, onNotification);
                    return () -> completeScript("UNLISTEN " + channel)
                            .thenAccept(res -> unsubscribe.run());
                });
    }

    @Override
    public CompletableFuture<Void> close() {
        return stream.close();
    }

    private static Columns calcColumns(ColumnDescription[] descriptions) {
        Map<String, PgColumn> byName = new HashMap<>();
        PgColumn[] ordered = new PgColumn[descriptions.length];
        for (int i = 0; i < descriptions.length; i++) {
            PgColumn column = new PgColumn(i, descriptions[i].getName(), descriptions[i].getType());
            byName.put(descriptions[i].getName(), column);
            ordered[i] = column;
        }
        return new Columns(Collections.unmodifiableMap(byName), ordered);
    }

    /**
     * Transaction that rollbacks the tx on backend error and closes the connection on COMMIT/ROLLBACK failure.
     */
    class PgConnectionTransaction implements Transaction {

        @Override
        public CompletableFuture<Transaction> begin() {
            return completeScript("SAVEPOINT sp_1")
                    .thenApply(rs -> new PgConnectionNestedTransaction(1));
        }

        CompletableFuture<Void> sendCommit() {
            return PgConnection.this.completeScript("COMMIT").thenAccept(readyForQuery -> {
            });
        }

        CompletableFuture<Void> sendRollback() {
            return PgConnection.this.completeScript("ROLLBACK").thenAccept(readyForQuery -> {
            });
        }

        @Override
        public CompletableFuture<Void> commit() {
            return sendCommit().thenAccept(rs -> {
            });
        }

        @Override
        public CompletableFuture<Void> rollback() {
            return sendRollback();
        }

        @Override
        public CompletableFuture<Void> close() {
            return sendCommit()
                    .handle((v, th) -> {
                        if (th != null) {
                            Logger.getLogger(PgConnectionTransaction.class.getName()).log(Level.SEVERE, null, th);
                            return sendRollback();
                        } else {
                            return CompletableFuture.completedFuture(v);
                        }
                    })
                    .thenCompose(Function.identity());
        }

        @Override
        public Connection getConnection() {
            return PgConnection.this;
        }

        @Override
        public CompletableFuture<Void> script(BiConsumer<Map<String, PgColumn>, PgColumn[]> onColumns, Consumer<Row> onRow, Consumer<Integer> onAffected, String sql) {
            return PgConnection.this.script(onColumns, onRow, onAffected, sql)
                    .handle((v, th) -> {
                        if (th != null) {
                            return rollback()
                                    .thenAccept(_v -> {
                                        throw new RuntimeException(th);
                                    });
                        } else {
                            return CompletableFuture.<Void>completedFuture(null);
                        }
                    })
                    .thenCompose(Function.identity());
        }

        public CompletableFuture<Integer> query(BiConsumer<Map<String, PgColumn>, PgColumn[]> onColumns, Consumer<Row> onRow, String sql, Object... params) {
            return PgConnection.this.query(onColumns, onRow, sql, params)
                    .handle((affected, th) -> {
                        if (th != null) {
                            return rollback()
                                    .<Integer>thenApply(v -> {
                                        throw new RuntimeException(th);
                                    });
                        } else {
                            return CompletableFuture.completedFuture(affected);
                        }
                    })
                    .thenCompose(Function.identity());
        }

    }

    /**
     * Nested transaction using savepoints.
     */
    class PgConnectionNestedTransaction extends PgConnectionTransaction {

        final int depth;

        PgConnectionNestedTransaction(int depth) {
            this.depth = depth;
        }

        @Override
        public CompletableFuture<Transaction> begin() {
            return completeScript("SAVEPOINT sp_" + (depth + 1))
                    .thenApply(rs -> new PgConnectionNestedTransaction(depth + 1));
        }

        @Override
        public CompletableFuture<Void> commit() {
            return PgConnection.this.completeScript("RELEASE SAVEPOINT sp_" + depth)
                    .thenAccept(rs -> {
                    });
        }

        @Override
        public CompletableFuture<Void> rollback() {
            return PgConnection.this.completeScript("ROLLBACK TO SAVEPOINT sp_" + depth)
                    .thenAccept(rs -> {
                    });
        }
    }
}
