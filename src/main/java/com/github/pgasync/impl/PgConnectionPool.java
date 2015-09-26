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
import com.github.pgasync.ConnectionPoolBuilder.PoolProperties;
import com.github.pgasync.impl.conversion.DataConverter;
import rx.Observable;
import rx.Subscriber;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Pool for backend connections. Callbacks are queued and executed when pool has an available
 * connection.
 *
 * TODO: Locking scheme is optimized for small thread pools and doesn't scale all that well
 * for large ones.
 *
 * @author Antti Laisi
 */
public abstract class PgConnectionPool implements ConnectionPool {

    final Queue<Subscriber<? super Connection>> waiters = new LinkedList<>();
    final Queue<Connection> connections = new LinkedList<>();
    final Object lock = new Object[0];

    final Map<String,Connection> listeners = new ConcurrentHashMap<>();

    final InetSocketAddress address;
    final String username;
    final String password;
    final String database;
    final DataConverter dataConverter;
    final ConnectionValidator validator;

    final int poolSize;
    protected final boolean pipeline;

    int currentSize;
    volatile boolean closed;

    public PgConnectionPool(PoolProperties properties) {
        this.address = new InetSocketAddress(properties.getHostname(), properties.getPort());
        this.username = properties.getUsername();
        this.password = properties.getPassword();
        this.database = properties.getDatabase();
        this.poolSize = properties.getPoolSize();
        this.dataConverter = properties.getDataConverter();
        this.validator = properties.getValidator();
        this.pipeline = properties.getUsePipelining();
    }

    @Override
    public Observable<Row> queryRows(String sql, Object... params) {
        return getConnection()
                .doOnNext(this::releaseIfPipelining)
                .flatMap(connection -> connection.queryRows(sql, params)
                        .doOnError(t -> releaseIfNotPipelining(connection))
                        .doOnCompleted(() -> releaseIfNotPipelining(connection)));
    }

    @Override
    public Observable<ResultSet> querySet(String sql, Object... params) {
        return getConnection()
                .doOnNext(this::releaseIfPipelining)
                .flatMap(connection -> connection.querySet(sql, params)
                        .doOnError(t -> releaseIfNotPipelining(connection))
                        .doOnCompleted(() -> releaseIfNotPipelining(connection)));
    }

    @Override
    public Observable<Transaction> begin() {
        return getConnection()
                .flatMap(connection -> connection.begin()
                        .doOnError(t -> release(connection))
                        .map(tx -> new ReleasingTransaction(connection, tx)));
    }

    @Override
    public Observable<String> listen(String channel) {
        return getConnection()
                .flatMap(connection -> connection.listen(channel)
                                        .doOnSubscribe(() -> release(connection)));
    }

    @Override
    public void close() {
        closed = true;
        synchronized (lock) {
            for(Connection conn = connections.poll(); conn != null; conn = connections.poll()) {
                conn.close();
                // TODO: remove conn from listeners
            }
            for(Subscriber<? super Connection> waiter = waiters.poll(); waiter != null; waiter = waiters.poll()) {
                waiter.onError(new SqlException("Connection pool is closed"));
            }
        }
    }

    @Override
    public Observable<Connection> getConnection() {
        if(closed) {
            return Observable.error(new SqlException("Connection pool is closed"));
        }

        return Observable.create(subscriber -> {

            Connection connection;

            synchronized (lock) {
                connection = connections.poll();
                if (connection == null) {
                    if (currentSize < poolSize) {
                        currentSize++;
                    } else {
                        waiters.add(subscriber);
                        return;
                    }
                }
            }

            if (connection != null) {
                subscriber.onNext(connection);
                subscriber.onCompleted();
                return;
            }

            new PgConnection(openStream(address), dataConverter)
                    .connect(username, password, database)
                    .subscribe(subscriber);
        });
    }

    private void releaseIfPipelining(Connection connection) {
        if (pipeline) {
            release(connection);
        }
    }

    private void releaseIfNotPipelining(Connection connection) {
        if (!pipeline) {
            release(connection);
        }
    }

    @Override
    public void release(Connection connection) {
        if(closed) {
            connection.close();
            return;
        }

        Subscriber<? super Connection> next;
        boolean failed;

        synchronized (lock) {
            failed = !((PgConnection) connection).isConnected();
            next = waiters.poll();
            if (next == null) {
                if(failed) {
                    currentSize--;
                } else {
                    connections.add(connection);
                }
            }
        }

        if(next == null) {
            return;
        }
        if(failed) {
            getConnection().subscribe(next);
            return;
        }

        next.onNext(connection);
        next.onCompleted();
    }

    /**
     * Creates a new socket stream to the backend.
     *
     * @param address Server address
     * @return Stream with no pending messages
     */
    protected abstract PgProtocolStream openStream(InetSocketAddress address);

    /*
    void validateAndApply(Connection connection, Consumer<Connection> onConnection, Consumer<Throwable> onError, int attempt) {

        Runnable onValid = () -> {
            try {
                onConnection.accept(connection);
            } catch (Throwable t) {
                release(connection);
                onError.accept(t);
            }
        };

        Consumer<Throwable> onValidationFailed = err -> {
            if(attempt > poolSize) {
                onError.accept(err);
                return;
            }
            try {
                connection.close();
            } catch (Throwable t) { /* ignored / }
            release(connection);
            getConnection(onConnection, onError, attempt + 1);
        };

        validator.validate(connection, onValid, onValidationFailed);
    }
    */

    /**
     * Transaction that rollbacks the tx on backend error and chains releasing the connection after COMMIT/ROLLBACK.
     */
    class ReleasingTransaction implements Transaction {
        Connection txconn;
        final Transaction transaction;

        ReleasingTransaction(Connection txconn, Transaction transaction) {
            this.txconn = txconn;
            this.transaction = transaction;
        }

        @Override
        public Observable<Void> rollback() {
            return transaction.rollback()
                    .doOnCompleted(this::releaseConnection)
                    .doOnError(exception -> closeAndRelease());
        }

        @Override
        public Observable<Void> commit() {
            return transaction.commit()
                    .doOnCompleted(this::releaseConnection)
                    .onErrorResumeNext(this::doRollback);
        }

        @Override
        public Observable<Row> queryRows(String sql, Object... params) {
            if (txconn == null) {
                return Observable.error(new SqlException("Transaction is already completed"));
            }
            return txconn.queryRows(sql)
                    .onErrorResumeNext(this::doRollback);
        }

        @Override
        public Observable<ResultSet> querySet(String sql, Object... params) {
            if (txconn == null) {
                return Observable.error(new SqlException("Transaction is already completed"));
            }
            return txconn.querySet(sql, params)
                    .onErrorResumeNext(this::doRollback);
        }

        void releaseConnection() {
            release(txconn);
            txconn = null;
        }
        void closeAndRelease() {
            txconn.close();
            releaseConnection();
        }

        <T> Observable<T> doRollback(Throwable exception) {
            if (!((PgConnection) txconn).isConnected()) {
                release(txconn);
                txconn = null;
                return Observable.error(exception);
            }

            return transaction.rollback()
                    .doOnError(rollbackException -> closeAndRelease())
                    .flatMap(__ -> Observable.error(exception));
        }
    }
}
