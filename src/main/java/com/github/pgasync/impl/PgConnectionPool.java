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

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

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

    static class QueuedCallback {
        final Consumer<Connection> connectionHandler;
        final Consumer<Throwable> errorHandler;
        QueuedCallback(Consumer<Connection> connectionHandler, Consumer<Throwable> errorHandler) {
            this.connectionHandler = connectionHandler;
            this.errorHandler = errorHandler;
        }
    }

    final Queue<QueuedCallback> waiters = new LinkedList<>();
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
    public void query(String sql, Consumer<ResultSet> onResult, Consumer<Throwable> onError) {
        query(sql, null, onResult, onError);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void query(String sql, List params, Consumer<ResultSet> onResult, Consumer<Throwable> onError) {
        getConnection(connection ->
        {
                        connection.query(sql, params,
                                result -> {
                                    releaseIfNotPipelining(connection);
                                    onResult.accept(result);
                                },
                                exception -> {
                                    releaseIfNotPipelining(connection);
                                    onError.accept(exception);
                                });
                        releaseIfPipelining(connection);
        },
                onError);
    }

    @Override
    public void begin(Consumer<Transaction> onTransaction, Consumer<Throwable> onError) {
        getConnection(connection ->
                        connection.begin(
                                transaction ->
                                        onTransaction.accept(new ReleasingTransaction(connection, transaction)),
                                exception -> {
                                    release(connection);
                                    onError.accept(exception);
                                }),
                onError);

    }

    @Override
    public void listen(String channel, Consumer<String> onNotification, Consumer<String> onListenStarted, Consumer<Throwable> onError) {
        getConnection(connection -> connection.listen(channel, onNotification,
                token -> {
                    listeners.put(token, connection);
                    onListenStarted.accept(token);
                }, onError), onError);
    }

    @Override
    public void unlisten(String channel, String unlistenToken, Runnable onListenStopped, Consumer<Throwable> onError) {
        Connection connection = listeners.get(unlistenToken);
        if(connection == null) {
            onError.accept(new IllegalStateException("Connection token does not map to a connection"));
            return;
        }
        connection.unlisten(channel, unlistenToken, onListenStopped, onError);
    }

    @Override
    public void close() {
        closed = true;
        synchronized (lock) {
            for(Connection conn = connections.poll(); conn != null; conn = connections.poll()) {
                conn.close();
                // TODO: remove conn from listeners
            }
            for(QueuedCallback waiter = waiters.poll(); waiter != null; waiter = waiters.poll()) {
                waiter.errorHandler.accept(new SqlException("Connection pool is closed"));
            }
        }
    }

    @Override
    public void getConnection(final Consumer<Connection> onConnection, final Consumer<Throwable> onError) {
        getConnection(onConnection, onError, 0);
    }

    void getConnection(final Consumer<Connection> onConnection, final Consumer<Throwable> onError, final int attempt) {
        if(closed) {
            onError.accept(new SqlException("Connection pool is closed"));
            return;
        }

        Connection connection;
        synchronized (lock) {
            connection = connections.poll();
            if (connection == null) {
                if (currentSize < poolSize) {
                    currentSize++;
                } else {
                    waiters.add(new QueuedCallback(onConnection, onError));
                    return;
                }
            }
        }

        if (connection != null) {
            validateAndApply(connection, onConnection, onError, attempt);
            return;
        }

        new PgConnection(openStream(address), dataConverter)
                .connect(username, password, database)
                .subscribe(onConnection::accept, onError::accept);
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

        QueuedCallback next;
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
        if (next != null) {
            if(failed) {
                getConnection(next.connectionHandler, next.errorHandler);
            } else {
                validateAndApply(connection, next.connectionHandler, next.errorHandler, 0);
            }
        }
    }

    /**
     * Creates a new socket stream to the backend.
     *
     * @param address Server address
     * @return Stream with no pending messages
     */
    protected abstract PgProtocolStream openStream(InetSocketAddress address);

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
            } catch (Throwable t) { /* ignored */ }
            release(connection);
            getConnection(onConnection, onError, attempt + 1);
        };

        validator.validate(connection, onValid, onValidationFailed);
    }

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
        public void rollback(Runnable onCompleted, Consumer<Throwable> onRollbackError) {
            transaction.rollback(
                    () -> {
                        release(txconn);
                        txconn = null;
                        onCompleted.run();
                    },
                    exception -> {
                        closeAndRelease();
                        onRollbackError.accept(exception);
                    });
        }

        @Override
        public void commit(Runnable onCompleted, Consumer<Throwable> onCommitError) {
            transaction.commit(
                    () -> {
                        release(txconn);
                        txconn = null;
                        onCompleted.run();
                    },
                    exception -> {
                        closeAndRelease();
                        onCommitError.accept(exception);
                    });
        }

        @Override
        @SuppressWarnings("rawtypes")
        public void query(String sql, List params, Consumer<ResultSet> onResult, Consumer<Throwable> onError) {
            if (txconn == null) {
                onError.accept(new SqlException("Transaction is rolled back"));
                return;
            }
            txconn.query(sql, params, onResult, exception -> doRollback(exception, onError));
        }

        @Override
        public void query(String sql, Consumer<ResultSet> onResult, Consumer<Throwable> onError) {
            query(sql, null, onResult, onError);
        }

        void closeAndRelease() {
            txconn.close();
            release(txconn);
            txconn = null;
        }

        void doRollback(Throwable exception, Consumer<Throwable> onError) {
            if (!((PgConnection) txconn).isConnected()) {
                release(txconn);
                txconn = null;
                onError.accept(exception);
                return;
            }

            transaction.rollback(() -> {
                        release(txconn);
                        txconn = null;
                        onError.accept(exception);
                    },
                    rollbackException -> {
                        closeAndRelease();
                        onError.accept(rollbackException);
                    });
        }
    }
}
