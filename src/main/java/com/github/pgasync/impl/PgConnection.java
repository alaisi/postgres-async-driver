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

import static com.github.pgasync.impl.message.backend.RowDescription.ColumnDescription;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.pgasync.Connection;
import com.github.pgasync.Listening;
import com.github.pgasync.PreparedStatement;
import com.github.pgasync.ResultSet;
import com.github.pgasync.Row;
import com.github.pgasync.Transaction;
import com.github.pgasync.impl.conversion.DataConverter;
import com.github.pgasync.impl.message.backend.Authentication;
import com.github.pgasync.impl.message.backend.RowDescription;
import com.github.pgasync.impl.message.frontend.Bind;
import com.github.pgasync.impl.message.frontend.Close;
import com.github.pgasync.impl.message.frontend.Describe;
import com.github.pgasync.impl.message.frontend.Execute;
import com.github.pgasync.impl.message.Message;
import com.github.pgasync.impl.message.frontend.Parse;
import com.github.pgasync.impl.message.frontend.PasswordMessage;
import com.github.pgasync.impl.message.frontend.Query;
import com.github.pgasync.impl.message.frontend.StartupMessage;

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
        private final String pname;

        PgPreparedStatement(String sname, String pname) {
            this.sname = sname;
            this.pname = pname;
        }

        @Override
        public CompletableFuture<ResultSet> query(Object... params) {
            AtomicReference<Map<String, PgColumn>> columnsRef = new AtomicReference<>();
            List<Row> rows = new ArrayList<>();
            return fetch(columnsRef::set, rows::add, params)
                    .thenApply(v -> new PgResultSet(columnsRef.get(), rows, 0));
        }

        @Override
        public CompletableFuture<Integer> fetch(Consumer<Map<String, PgColumn>> onColumns, Consumer<Row> processor, Object... params) {
            return stream
                    .send(new Bind(sname, pname, dataConverter.fromParameters(params)))
                    .thenApply(bindComplete -> stream.send(Describe.portal(pname)))
                    .thenCompose(Function.identity())
                    .thenApply(rowDescription -> calcColumns(((RowDescription) rowDescription).getColumns()))
                    .thenApply(columns -> {
                        onColumns.accept(columns);
                        return stream.send(new Execute(pname), dataRow -> processor.accept(new PgRow(dataRow, columns, dataConverter)));
                    })
                    .thenCompose(Function.identity());
        }

        @Override
        public CompletableFuture<Void> close() {
            return stream.send(Close.statement(sname)).thenAccept(closeComplete -> {
            });
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
    private static final NameSequence portalNames = new NameSequence("p-");

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
                .thenApply(parseComplete -> new PgPreparedStatement(statementName, portalNames.next()));
    }

    @Override
    public CompletableFuture<Void> script(Consumer<Map<String, PgColumn>> onColumns, Consumer<Row> onRow, Consumer<Integer> onAffected, String sql) {
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("'sql' shouldn't be null or empty or blank string");
        }
        AtomicReference<Map<String, PgColumn>> columnsRef = new AtomicReference<>();
        return stream.send(
                new Query(sql),
                columnDescriptions -> {
                    Map<String, PgColumn> columns = calcColumns(columnDescriptions);
                    onColumns.accept(columns);
                    columnsRef.set(columns);
                },
                message -> onRow.accept(new PgRow(message, columnsRef.get(), dataConverter)),
                message -> onAffected.accept(message.getAffectedRows())
        );
    }

    @Override
    public CompletableFuture<Integer> query(Consumer<Map<String, PgColumn>> onColumns, Consumer<Row> onRow, String sql, Object... params) {
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

    private static Map<String, PgColumn> calcColumns(ColumnDescription[] descriptions) {
        Map<String, PgColumn> columns = new LinkedHashMap<>();
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
        public CompletableFuture<Void> script(Consumer<Map<String, PgColumn>> onColumns, Consumer<Row> onRow, Consumer<Integer> onAffected, String sql) {
            return PgConnection.this.script(onColumns, onRow, onAffected, sql)
                    .exceptionally(th -> {
                        rollback();
                        throw new RuntimeException(th);
                    });
        }

        public CompletableFuture<Integer> query(Consumer<Map<String, PgColumn>> onColumns, Consumer<Row> onRow, String sql, Object... params) {
            return PgConnection.this.query(onColumns, onRow, sql, params)
                    .exceptionally(th -> {
                        rollback();
                        throw new RuntimeException(th);
                    });
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
