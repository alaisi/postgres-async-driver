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
import rx.Subscriber;
import rx.Subscription;
import rx.subscriptions.Subscriptions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.pgasync.impl.message.RowDescription.ColumnDescription;

/**
 * A connection to PostgreSQL backed. The postmaster forks a backend process for
 * each connection. A connection can process only a single queryRows at a time.
 * 
 * @author Antti Laisi
 */
public class PgConnection implements Connection, Transaction {

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
                    ? stream.authenticate(new PasswordMessage(username, password, ((Authentication) message).getMd5Salt()))
                    : Observable.just(message);
    }

    boolean isConnected() {
        return stream.isConnected();
    }

    @Override
    public Observable<ResultSet> querySet(String sql, Object... params) {
        return sendQuery(sql, params)
                .lift(toResultSet(dataConverter));
    }

    @Override
    public Observable<Row> queryRows(String sql, Object... params) {
        return sendQuery(sql, params)
                .lift(toRow(dataConverter));
    }

    @Override
    public Observable<Transaction> begin() {
        return queryRows("BEGIN").map(row -> this);
    }

    @Override
    public Observable<Void> commit() {
        return queryRows("COMMIT").map(row -> null);
    }

    @Override
    public Observable<Void> rollback() {
        return queryRows("ROLLBACK").map(row -> null);
    }

    @Override
    public Observable<String> listen(String channel) {
        AtomicReference<String> token = new AtomicReference<>();
        return Observable.<String>create(subscriber ->

                querySet("LISTEN " + channel)
                        .subscribe( rs -> token.set(stream.registerNotificationHandler(channel, subscriber::onNext)),
                                    subscriber::onError)

        ).doOnUnsubscribe(() -> querySet("UNLISTEN " + channel)
                                    .subscribe(rs -> stream.unRegisterNotificationHandler(channel, token.get())));
    }

    @Override
    public void close() {
        stream.close();
    }

    private Observable<Message> sendQuery(String sql, Object[] params) {
        return params == null || params.length == 0
                ? stream.send(
                    new Query(sql))
                : stream.send(
                    new Parse(sql),
                    new Bind(dataConverter.fromParameters(params)),
                    ExtendedQuery.DESCRIBE,
                    ExtendedQuery.EXECUTE,
                    ExtendedQuery.CLOSE,
                    ExtendedQuery.SYNC);
    }

    static Observable.Operator<Row,? super Message> toRow(DataConverter dataConverter) {
        return subscriber -> new Subscriber<Message>() {

            Map<String, PgColumn> columns;

            @Override
            public void onNext(Message message) {
                if (message instanceof RowDescription) {
                    columns = getColumns(((RowDescription) message).getColumns());
                } else if(message instanceof DataRow) {
                    subscriber.onNext(new PgRow((DataRow) message, columns, dataConverter));
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
    }

    static Observable.Operator<ResultSet,? super Message> toResultSet(DataConverter dataConverter) {
        return subscriber -> new Subscriber<Message>() {

            Map<String, PgColumn> columns;
            List<Row> rows = new ArrayList<>();
            int updated;

            @Override
            public void onNext(Message message) {
                if (message instanceof RowDescription) {
                    columns = getColumns(((RowDescription) message).getColumns());
                } else if(message instanceof DataRow) {
                    rows.add(new PgRow((DataRow) message, columns, dataConverter));
                } else if(message instanceof CommandComplete) {
                    updated = ((CommandComplete) message).getUpdatedRows();
                } else if(message == ReadyForQuery.INSTANCE) {
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
    }

    static Map<String,PgColumn> getColumns(ColumnDescription[] descriptions) {
        Map<String,PgColumn> columns = new HashMap<>();
        for (int i = 0; i < descriptions.length; i++) {
            columns.put(descriptions[i].getName().toUpperCase(), new PgColumn(i, descriptions[i].getType()));
        }
        return columns;
    }

}
