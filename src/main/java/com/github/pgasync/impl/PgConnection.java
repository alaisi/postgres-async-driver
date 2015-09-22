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

import com.github.pgasync.Connection;
import com.github.pgasync.ResultSet;
import com.github.pgasync.Row;
import com.github.pgasync.Transaction;
import com.github.pgasync.impl.conversion.DataConverter;
import com.github.pgasync.impl.message.*;
import rx.Observable;
import rx.functions.Func1;

import java.util.*;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;
import java.util.function.Consumer;

import static com.github.pgasync.impl.Functions.applyConsumer;
import static com.github.pgasync.impl.Functions.applyRunnable;
import static com.github.pgasync.impl.message.RowDescription.ColumnDescription;

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

    Observable<Connection> connect(String username, String password, String database) {
       return stream.connect(new StartupMessage(username, database))
               .flatMap(message -> authenticate(username, password, message))
               .filter(ReadyForQuery.class::isInstance)
               .map(ready -> this);
    }

    Observable<? extends Message> authenticate(String username, String password, Message message) {
        return message instanceof Authentication
                ? stream.send(new PasswordMessage(username, password, ((Authentication) message).getMd5Salt()))
                : Observable.just(message);
    }

    boolean isConnected() {
        return stream.isConnected();
    }

    @Override
    public void query(String sql, Consumer<ResultSet> onQuery, Consumer<Throwable> onError) {
        stream.send(new Query(sql))
                .reduce(new QueryResponse(), this::toResponse)
                .map(this::toResultSet)
                .subscribe(onQuery::accept, onError::accept);
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void query(String sql, List params, Consumer<ResultSet> onQuery, Consumer<Throwable> onError) {
        if (params == null || params.isEmpty()) {
            query(sql, onQuery, onError);
            return;
        }
        stream.send(new Parse(sql), new Bind(dataConverter.fromParameters(params)),
                    ExtendedQuery.DESCRIBE, ExtendedQuery.EXECUTE, ExtendedQuery.CLOSE, ExtendedQuery.SYNC)
                .reduce(new QueryResponse(), this::toResponse)
                .map(this::toResultSet)
                .subscribe(onQuery::accept, onError::accept);
    }

    @Override
    public void begin(Consumer<Transaction> handler, Consumer<Throwable> onError) {
        query("BEGIN", beginRs -> applyConsumer(handler, new ConnectionTx(), onError), onError);
    }

    @Override
    public void listen(String channel, Consumer<String> onNotification, Consumer<String> onListenStarted, Consumer<Throwable> onError) {
        stream.send(new Query("LISTEN " + channel))
                .subscribe(msg -> {
                }, onError::accept,
                        () -> onListenStarted.accept(stream.registerNotificationHandler(channel, onNotification)));
    }

    @Override
    public void unlisten(String channel, String unlistenToken, Runnable onListenStopped, Consumer<Throwable> onError) {
        stream.send(new Query("UNLISTEN " + channel))
                .subscribe(msg -> {}, onError::accept,
                        () -> {
                            stream.unRegisterNotificationHandler(channel, unlistenToken);
                            onListenStopped.run();
                        });
    }

    @Override
    public void close() {
        stream.close();
    }

    public Observable<Row> query(String sql) {
        return stream.send(new Query(sql))
                .map(mapRowFn())
                .filter(msg -> msg != null)
                .map(row -> new PgRow(row.getKey(), row.getValue(), dataConverter));
    }

    Func1<? super Message, Entry<DataRow,Map<String,PgColumn>>> mapRowFn() {
        Map<String, PgColumn> columns = new HashMap<>();
        return msg -> {
            if (msg instanceof RowDescription) {
                columns.putAll(getColumns(((RowDescription) msg).getColumns()));
                return null;
            }
            return msg instanceof DataRow
                    ? new SimpleImmutableEntry<>((DataRow) msg, columns)
                    : null;
        };
    }

    QueryResponse toResponse(QueryResponse response, Message message) {
        if (message instanceof RowDescription) {
            response.columns = getColumns(((RowDescription) message).getColumns());
        } else if (message instanceof DataRow) {
            response.rows.add(new PgRow((DataRow) message, response.columns, dataConverter));
        } else if (message instanceof CommandComplete) {
            response.updated = ((CommandComplete) message).getUpdatedRows();
        }
        return response;
    }

    PgResultSet toResultSet(QueryResponse response) {
        return new PgResultSet(response.columns, response.rows, response.updated);
    }

    static Map<String,PgColumn> getColumns(ColumnDescription[] descriptions) {
        Map<String,PgColumn> columns = new HashMap<>();
        for (int i = 0; i < descriptions.length; i++) {
            columns.put(descriptions[i].getName().toUpperCase(), new PgColumn(i, descriptions[i].getType()));
        }
        return columns;
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

    static class QueryResponse {
        Map<String, PgColumn> columns;
        List<Row> rows = new ArrayList<>();
        int updated;
    }
}
