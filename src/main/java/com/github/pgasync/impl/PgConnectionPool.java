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
import rx.observers.Subscribers;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Pool for backend connections. Callbacks are queued and executed when pool has an available
 * connection.
 *
 * @author Antti Laisi
 */
public abstract class PgConnectionPool implements ConnectionPool {

    final int poolSize;
    final AtomicInteger currentSize = new AtomicInteger();
    final BlockingQueue<Subscriber<? super Connection>> subscribers = new LinkedBlockingQueue<>();
    final BlockingQueue<Connection> connections = new LinkedBlockingQueue<>();

    final InetSocketAddress address;
    final String username;
    final String password;
    final String database;
    final DataConverter dataConverter;
    final ConnectionValidator validator;
    final boolean pipeline;

    final AtomicBoolean closed = new AtomicBoolean();

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
    public void close() {
        closed.set(true);

        while(!subscribers.isEmpty()) {
            Subscriber<? super Connection> subscriber = subscribers.poll();
            if(subscriber != null) {
                subscriber.onError(new SqlException("Connection pool is closing"));
            }
        }

        try {
            while (currentSize.get() > 0) {
                Connection connection = connections.poll(10, SECONDS);
                if(connection == null) {
                    break;
                }
                connection.close();
                currentSize.decrementAndGet();
            }
        } catch (InterruptedException e) { /* ignore */ }
    }

    @Override
    public Observable<Connection> getConnection() {
        if(closed.get()) {
            return Observable.error(new SqlException("Connection pool is closed"));
        }

        return Observable.create(subscriber -> {

            Connection connection = connections.poll();
            if (connection != null) {
                subscriber.onNext(connection);
                subscriber.onCompleted();
                return;
            }

            if(!tryIncreaseSize()) {
                subscribers.add(subscriber);
                return;
            }

            new PgConnection(openStream(address), dataConverter)
                    .connect(username, password, database)
                    .subscribe(subscriber);
        });
    }

    private boolean tryIncreaseSize() {
        while(true) {
            final int current = currentSize.get();
            if(current == poolSize) {
                return false;
            }
            if(currentSize.compareAndSet(current, current + 1)) {
                return true;
            }
        }
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

        if(closed.get()) {
            connection.close();
            return;
        }

        boolean failed = !((PgConnection) connection).isConnected();
        Subscriber<? super Connection> next = subscribers.poll();

        if(next == null) {
            if(failed) {
                currentSize.decrementAndGet();
            } else {
                connections.add(connection);
            }
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
            return transaction.queryRows(sql)
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
