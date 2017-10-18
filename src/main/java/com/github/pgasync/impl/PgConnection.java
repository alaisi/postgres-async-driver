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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.observers.Subscribers;

/**
 * A connection to PostgreSQL backed. The postmaster forks a backend process for
 * each connection. A connection can process only a single queryRows at a time.
 * 
 * @author Antti Laisi
 */
public class PgConnection implements Connection {
    static final Logger LOG = LoggerFactory.getLogger(PgConnection.class);
    final PgProtocolStream stream;
    final DataConverter dataConverter;

    public PgConnection(PgProtocolStream stream, DataConverter dataConverter) {
        this.stream = stream;
        this.dataConverter = dataConverter;
    }

    Observable<Connection> connect(String username, String password, String database) {
       return stream.connect(new StartupMessage(username, database))
               .flatMap(message -> authenticate(username, password, message))
               .single(message -> message == ReadyForQuery.INSTANCE)
               .map(ready -> this);
    }

    Observable<? extends Message> authenticate(String username, String password, Message message) {
        return message instanceof Authentication && !((Authentication) message).isAuthenticationOk()
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
        return querySet("BEGIN").map(rs -> new PgConnectionTransaction());
    }

    @Override
    public Observable<String> listen(String channel) {
        // TODO: wait for commit before sending unlisten as otherwise it can be rolled back
        return querySet("LISTEN " + channel)
                .<String>lift(subscriber -> Subscribers.create( rs -> stream.listen(channel)
                                                                        .subscribe(subscriber),
                                                                subscriber::onError))
                .doOnUnsubscribe(() -> querySet("UNLISTEN " + channel).subscribe(rs -> { }));
    }

    @Override
    public void close() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        stream.close().subscribe(__ -> latch.countDown(), ex -> {
            LOG.warn("Exception closing connection", ex);
            latch.countDown();
        });
        latch.await(1000, TimeUnit.MILLISECONDS);
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
        return subscriber -> new Subscriber<Message>(subscriber) {

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

    /**
     * Transaction that rollbacks the tx on backend error and closes the connection on COMMIT/ROLLBACK failure.
     */
    class PgConnectionTransaction implements Transaction {

        @Override
        public Observable<Transaction> begin() {
            return querySet("SAVEPOINT sp_1").map(rs -> new PgConnectionNestedTransaction(1));
        }
        @Override
        public Observable<Void> commit() {
            return PgConnection.this.querySet("COMMIT")
                    .map(rs -> (Void) null)
                    .doOnError(exception -> stream.close().subscribe());
        }
        @Override
        public Observable<Void> rollback() {
            return PgConnection.this.querySet("ROLLBACK")
                    .map(rs -> (Void) null)
                    .doOnError(exception -> stream.close().subscribe());
        }
        @Override
        public Observable<Row> queryRows(String sql, Object... params) {
            return PgConnection.this.queryRows(sql, params)
                    .onErrorResumeNext(this::doRollback);
        }
        @Override
        public Observable<ResultSet> querySet(String sql, Object... params) {
            return PgConnection.this.querySet(sql, params)
                    .onErrorResumeNext(this::doRollback);
        }
        <T> Observable<T> doRollback(Throwable t) {
            return rollback().flatMap(__ -> Observable.error(t));
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
        public Observable<Transaction> begin() {
            return querySet("SAVEPOINT sp_" + (depth+1))
                    .map(rs -> new PgConnectionNestedTransaction(depth+1));
        }
        @Override
        public Observable<Void> commit() {
            return PgConnection.this.querySet("RELEASE SAVEPOINT sp_" + depth)
                    .map(rs -> (Void) null);
        }
        @Override
        public Observable<Void> rollback() {
            return PgConnection.this.querySet("ROLLBACK TO SAVEPOINT sp_" + depth)
                    .map(rs -> (Void) null);
        }
    }
}
