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

import com.github.pgasync.*;
import com.github.pgasync.impl.conversion.DataConverter;
import com.github.pgasync.impl.message.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.github.pgasync.impl.Functions.*;
import static com.github.pgasync.impl.message.RowDescription.ColumnDescription;
import static java.util.Arrays.asList;

/**
 * A connection to PostgreSQL backed. The postmaster forks a backend process for
 * each connection. A connection can process only a single query at a time.
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

    void connect(String username, String password, String database,
                        Consumer<Connection> onConnected, Consumer<Throwable> onError) {

        stream.connect(new StartupMessage(username, database), messages -> {
            if (fireErrorHandler(messages.stream(), onError)) {
                return;
            }
            if(messages.stream().anyMatch(m -> m instanceof ReadyForQuery)) {
                applyConsumer(onConnected, this, onError);
                return;
            }
            byte[] md5salt = reduce(new AuthenticationResponseReader(), messages.stream()).get();
            stream.send(new PasswordMessage(username, password, md5salt), pwMessages -> {
                if (fireErrorHandler(pwMessages.stream(), onError)) {
                    return;
                }
                applyConsumer(onConnected, this, onError);
            });
        });
    }

    boolean isConnected() {
        return stream.isConnected();
    }

    @Override
    public void query(String sql, Consumer<ResultSet> onQuery, Consumer<Throwable> onError) {
        stream.send(new Query(sql), messages -> {
            if (!fireErrorHandler(messages.stream(), onError)) {
                applyConsumer(onQuery, reduce(new QueryResponseReader(), messages.stream()).get(), onError);
            }
        });
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void query(String sql, List params, Consumer<ResultSet> onQuery, Consumer<Throwable> onError) {
        if (params == null || params.isEmpty()) {
            query(sql, onQuery, onError);
            return;
        }
        stream.send(
                asList(new Parse(sql),
                    new Bind(dataConverter.fromParameters(params)),
                    ExtendedQuery.DESCRIBE,
                    ExtendedQuery.EXECUTE,
                    ExtendedQuery.CLOSE,
                    ExtendedQuery.SYNC),
                messages -> {
                    if (!fireErrorHandler(messages.stream(), onError)) {
                        applyConsumer(onQuery, reduce(new QueryResponseReader(), messages.stream()).get(), onError);
                    }
                });
    }

    @Override
    public void begin(Consumer<Transaction> handler, Consumer<Throwable> onError) {
        query("BEGIN", beginRs -> applyConsumer(handler, new ConnectionTx(), onError), onError);
    }

    @Override
    public void close() {
        stream.close();
    }

    boolean fireErrorHandler(Stream<Message> messages, Consumer<Throwable> onError) {
        Message failure = messages
                .filter(m -> m instanceof ErrorResponse || m instanceof Throwable)
                .findFirst().orElse(null);

        if(failure != null && failure instanceof ErrorResponse) {
            ErrorResponse err = (ErrorResponse) failure;
            applyConsumer(onError, new SqlException(err.getLevel().name(), err.getCode(), err.getMessage()));
            return true;
        }
        if(failure != null) {
            applyConsumer(onError, (Throwable) failure);
            return true;
        }
        return false;
    }

    class QueryResponseReader implements Function<Message,QueryResponseReader>, Supplier<PgResultSet> {

        Map<String, PgColumn> columns;
        List<Row> rows = new ArrayList<>();
        int updated;

        @Override
        public PgResultSet get() {
            return new PgResultSet(columns, rows, updated);
        }

        @Override
        public QueryResponseReader apply(Message msg) {
            if(msg instanceof RowDescription) {
                columns = toColumns(((RowDescription) msg).getColumns());
            } else if (msg instanceof DataRow) {
                rows.add(new PgRow((DataRow) msg, columns, dataConverter));
            } else if(msg instanceof CommandComplete) {
                updated = ((CommandComplete) msg).getUpdatedRows();
            }
            return this;
        }

        Map<String,PgColumn> toColumns(ColumnDescription[] columnDescriptions) {
            Map<String,PgColumn> columns = new LinkedHashMap<>();
            for (int i = 0; i < columnDescriptions.length; i++) {
                columns.put(columnDescriptions[i].getName().toUpperCase(),
                        new PgColumn(i, columnDescriptions[i].getType()));
            }
            return columns;
        }
    }

    static class AuthenticationResponseReader implements Function<Message,AuthenticationResponseReader>, Supplier<byte[]> {

        byte[] md5salt;

        @Override
        public byte[] get() {
            return md5salt;
        }

        @Override
        public AuthenticationResponseReader apply(Message message) {
            if(md5salt == null && message instanceof Authentication) {
                md5salt = ((Authentication) message).getMd5Salt();
            }
            return this;
        }
    }

    class ConnectionTx implements Transaction {
        @Override
        public void commit(Runnable onCompleted, Consumer<Throwable> onCommitError) {
            PgConnection.this.query("COMMIT", (rs) -> applyRunnable(onCompleted, onCommitError), onCommitError);
        }
        @Override
        public void rollback(Runnable onCompleted, Consumer<Throwable> onRollbackError) {
            PgConnection.this.query("ROLLBACK", (rs) -> applyRunnable(onCompleted, onRollbackError), onRollbackError);
        }
        @Override
        public void query(String sql, Consumer<ResultSet> onResult, Consumer<Throwable> onError) {
            PgConnection.this.query(sql, onResult, onError);
        }
        @Override
        public void query(String sql, List params, Consumer<ResultSet> onResult, Consumer<Throwable> onError) {
            PgConnection.this.query(sql, params, onResult, onError);
        }
    }

}
