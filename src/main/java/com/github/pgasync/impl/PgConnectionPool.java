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
import java.util.Queue;
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

    final InetSocketAddress address;
    final String username;
    final String password;
    final String database;
    final DataConverter dataConverter;

    final int poolSize;
    int currentSize;
    volatile boolean closed;

    public PgConnectionPool(PoolProperties properties) {
        this.address = new InetSocketAddress(properties.getHostname(), properties.getPort());
        this.username = properties.getUsername();
        this.password = properties.getPassword();
        this.database = properties.getDatabase();
        this.poolSize = properties.getPoolSize();
        this.dataConverter = properties.getDataConverter();
    }

    @Override
    public void query(String sql, Consumer<ResultSet> onResult, Consumer<Throwable> onError) {
        query(sql, null, onResult, onError);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void query(String sql, List params, Consumer<ResultSet> onResult, Consumer<Throwable> onError) {
        getConnection(connection ->
                        connection.query(sql, params,
                                result -> {
                                    release(connection);
                                    onResult.accept(result);
                                },
                                exception -> {
                                    release(connection);
                                    onError.accept(exception);
                                }),
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
    public void close() {
        closed = true;
        synchronized (lock) {
            for(Connection conn = connections.poll(); conn != null; conn = connections.poll()) {
                conn.close();
            }
            for(QueuedCallback waiter = waiters.poll(); waiter != null; waiter = waiters.poll()) {
                waiter.errorHandler.accept(new SqlException("Connection pool is closed"));
            }
        }
    }

    @Override
    public void getConnection(final Consumer<Connection> onConnection, final Consumer<Throwable> onError) {
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

        if (connection == null) {
            newConnection(address).connect(username, password, database, onConnection, onError);
        } else {
            onConnection.accept(connection);
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
                next.connectionHandler.accept(connection);
            }
        }
    }

    protected DataConverter dataConverter() {
        return dataConverter;
    }

    /**
     * Creates a new connection to the backend.
     * 
     * @param address Server address
     * @return Unconnected connection
     */
    protected abstract PgConnection newConnection(InetSocketAddress address);

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
                        txconn.close();
                        release(txconn);
                        txconn = null;
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
                        txconn.close();
                        release(txconn);
                        txconn = null;
                        onCommitError.accept(exception);
                    });
        }

        @Override
        @SuppressWarnings("rawtypes")
        public void query(String sql, List params, Consumer<ResultSet> onResult, Consumer<Throwable> onError) {
            if(txconn == null) {
                onError.accept(new SqlException("Transaction is rolled back"));
                return;
            }
            txconn.query(sql, params, onResult,
                    exception ->
                            transaction.rollback(() -> {
                                release(txconn);
                                txconn = null;
                                onError.accept(exception);
                            },
                            rollbackException -> {
                                txconn.close();
                                release(txconn);
                                txconn = null;
                                onError.accept(rollbackException);
                            }));
        }
        @Override
        public void query(String sql, Consumer<ResultSet> onResult, Consumer<Throwable> onError) {
            query(sql, null, onResult, onError);
        }
    }

}
