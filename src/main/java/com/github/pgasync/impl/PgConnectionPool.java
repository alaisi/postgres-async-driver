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
import rx.functions.Func1;
import rx.observers.Subscribers;

import javax.annotation.concurrent.GuardedBy;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Pool for backend connections. Callbacks are queued and executed when pool has an available
 * connection.
 *
 * @author Antti Laisi
 */
public abstract class PgConnectionPool implements ConnectionPool {

    final int poolSize;
    final ReentrantLock lock = new ReentrantLock();
    @GuardedBy("lock")
    final Condition closingConnectionReleased = lock.newCondition();
    @GuardedBy("lock")
    int currentSize;
    @GuardedBy("lock")
    boolean closed;
    @GuardedBy("lock")
    final Queue<Subscriber<? super Connection>> subscribers = new LinkedList<>();
    @GuardedBy("lock")
    final Queue<Connection> connections = new LinkedList<>();

    final InetSocketAddress address;
    final String username;
    final String password;
    final String database;
    final DataConverter dataConverter;
    final Func1<Connection, Observable<Connection>> validator;
    final boolean pipeline;

    public PgConnectionPool(PoolProperties properties) {
        this.address = InetSocketAddress.createUnresolved(properties.getHostname(), properties.getPort());
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
                                        .doOnTerminate(() -> releaseIfNotPipelining(connection)));
    }

    @Override
    public Observable<ResultSet> querySet(String sql, Object... params) {
        return getConnection()
                .doOnNext(this::releaseIfPipelining)
                .flatMap(connection -> connection.querySet(sql, params)
                        .doOnTerminate(() -> releaseIfNotPipelining(connection)));
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
                .lift(subscriber ->
                        Subscribers.create(
                                connection -> connection.listen(channel)
                                        .doOnSubscribe(() -> release(connection))
                                        .onErrorResumeNext(exception -> listen(channel))
                                        .subscribe(subscriber),
                                subscriber::onError));
    }

    @Override
    public void close() throws Exception {
        lock.lock();
        try {
            closed = true;

            while(!subscribers.isEmpty()) {
                Subscriber<? super Connection> queued = subscribers.poll();
                if(queued != null) {
                    queued.onError(new SqlException("Connection pool is closing"));
                }
            }

            try {
                while (currentSize > 0) {
                    Connection connection = connections.poll();
                    if(connection == null) {
                        if (closingConnectionReleased.await(10, SECONDS)) {
                            break;
                        }
                        continue;
                    }
                    currentSize--;
                    connection.close();
                }
            } catch (InterruptedException e) { /* ignore */ }

        } finally {
            lock.unlock();
        }
    }

    @Override
    public Observable<Connection> getConnection() {
        return Observable.<Connection>create(subscriber -> {
            boolean locked = true;
            lock.lock();
            try {
                if (closed) {
                    lock.unlock();
                    locked = false;
                    subscriber.onError(new SqlException("Connection pool is closed"));
                    return;
                }

                Connection connection = connections.poll();
                if (connection != null) {
                    lock.unlock();
                    locked = false;
                    subscriber.onNext(connection);
                    subscriber.onCompleted();
                    return;
                }

                if(!tryIncreaseSize()) {
                    subscribers.add(subscriber);
                    return;
                }
                lock.unlock();
                locked = false;

                new PgConnection(openStream(address), dataConverter)
                        .connect(username, password, database)
                        .doOnError(__ -> release(null))
                        .subscribe(subscriber);
            } finally {
                if (locked) {
                    lock.unlock();
                }
            }
        }).flatMap(conn -> validator.call(conn).doOnError(err -> release(conn)))
                .retry(poolSize + 1);
    }

    private boolean tryIncreaseSize() {
        if(currentSize >= poolSize) {
            return false;
        }
        currentSize++;
        return true;
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
        boolean failed = connection == null || !((PgConnection) connection).isConnected();

        Subscriber<? super Connection> next;
        lock.lock();
        try {
            if(subscribers.isEmpty()) {
                if(failed) {
                    currentSize--;
                } else {
                    connections.add(connection);
                }
                if (closed) {
                    this.closingConnectionReleased.signalAll();
                }
                return;
            }

            next = subscribers.poll();
        } finally {
            lock.unlock();
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

    /**
     * Transaction that chains releasing the connection after COMMIT/ROLLBACK.
     */
    class ReleasingTransaction implements Transaction {

        final AtomicBoolean released = new AtomicBoolean();
        final Connection txconn;
        final Transaction transaction;

        ReleasingTransaction(Connection txconn, Transaction transaction) {
            this.txconn = txconn;
            this.transaction = transaction;
        }

        @Override
        public Observable<Transaction> begin() {
            // Nested transactions should not release things automatically.
            return transaction.begin();
        }

        @Override
        public Observable<Void> rollback() {
            return transaction.rollback()
                    .doOnTerminate(this::releaseConnection);
        }

        @Override
        public Observable<Void> commit() {
            return transaction.commit()
                    .doOnTerminate(this::releaseConnection);
        }

        @Override
        public Observable<Row> queryRows(String sql, Object... params) {
            if (released.get()) {
                return Observable.error(new SqlException("Transaction is already completed"));
            }
            return transaction.queryRows(sql, params)
                    .doOnError(exception -> releaseConnection());
        }

        @Override
        public Observable<ResultSet> querySet(String sql, Object... params) {
            if (released.get()) {
                return Observable.error(new SqlException("Transaction is already completed"));
            }
            return transaction.querySet(sql, params)
                    .doOnError(exception -> releaseConnection());
        }

        void releaseConnection() {
            release(txconn);
            released.set(true);
        }
    }
}
