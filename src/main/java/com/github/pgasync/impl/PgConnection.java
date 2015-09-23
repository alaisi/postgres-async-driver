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
import com.github.pgasync.Row;
import com.github.pgasync.Transaction;
import com.github.pgasync.impl.conversion.DataConverter;
import com.github.pgasync.impl.message.*;
import rx.Observable;
import rx.functions.Func1;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

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
    public Observable<Row> query(String sql) {
        return stream.send(new Query(sql))
                .flatMap(toDataRowFn())
                .map(row -> new PgRow(row.getKey(), row.getValue(), dataConverter));
    }

    @Override
    public Observable<Row> query(String sql, Object... params) {
        return stream.send( new Parse(sql),
                            new Bind(dataConverter.fromParameters(params)),
                            ExtendedQuery.DESCRIBE,
                            ExtendedQuery.EXECUTE,
                            ExtendedQuery.CLOSE,
                            ExtendedQuery.SYNC)
                .flatMap(toDataRowFn())
                .map(row -> new PgRow(row.getKey(), row.getValue(), dataConverter));
    }

    @Override
    public Observable<Transaction> begin() {
        return query("BEGIN").map(row -> new ConnectionTx());
    }

    @Override
    public void listen(String channel, Consumer<String> onNotification, Consumer<String> onListenStarted, Consumer<Throwable> onError) {
        stream.send(new Query("LISTEN " + channel))
                .subscribe(msg -> {}, onError::accept,
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

    Func1<? super Message, Observable<Entry<DataRow,Map<String,PgColumn>>>> toDataRowFn() {
        Map<String, PgColumn> columns = new HashMap<>();
        return message -> {
            if(message instanceof DataRow) {
                return Observable.just(new SimpleImmutableEntry<>((DataRow) message, columns));
            }
            if (message instanceof RowDescription) {
                columns.putAll(getColumns(((RowDescription) message).getColumns()));
            }
            return Observable.empty();
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
        public Observable<Void> commit() {
            return PgConnection.this.query("COMMIT").map(row -> null);
        }
        @Override
        public Observable<Void> rollback() {
            return PgConnection.this.query("ROLLBACK").map(row -> null);
        }
        @Override
        public Observable<Row> query(String sql) {
            return PgConnection.this.query(sql);
        }
        @Override
        public Observable<Row> query(String sql, Object... params) {
            return PgConnection.this.query(sql, params);
        }
    }

    static class QueryResponse {
        Map<String, PgColumn> columns;
        List<Row> rows = new ArrayList<>();
        int updated;
    }
}
