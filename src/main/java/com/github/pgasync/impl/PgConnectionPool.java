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

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import com.github.pgasync.Connection;
import com.github.pgasync.ConnectionPool;
import com.github.pgasync.ConnectionPoolBuilder.PoolProperties;
import com.github.pgasync.ResultSet;
import com.github.pgasync.SqlException;
import com.github.pgasync.Transaction;
import com.github.pgasync.callback.ChainedErrorHandler;
import com.github.pgasync.callback.ConnectionHandler;
import com.github.pgasync.callback.ErrorHandler;
import com.github.pgasync.callback.ResultHandler;
import com.github.pgasync.callback.TransactionCompletedHandler;
import com.github.pgasync.callback.TransactionHandler;

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
        final ConnectionHandler connectionHandler;
        final ErrorHandler errorHandler;
        QueuedCallback(ConnectionHandler connectionHandler, ErrorHandler errorHandler) {
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

    final int poolSize;
    int currentSize;
    volatile boolean closed;

    public PgConnectionPool(PoolProperties properties) {
        this.address = new InetSocketAddress(properties.getHostname(), properties.getPort());
        this.username = properties.getUsername();
        this.password = properties.getPassword();
        this.database = properties.getDatabase();
        this.poolSize = properties.getPoolSize();
    }

    @Override
    public void query(final String sql, final ResultHandler onResult, final ErrorHandler onError) {
        query(sql, null, onResult, onError);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void query(final String sql, final List params, final ResultHandler onResult, final ErrorHandler onError) {
        getConnection(connection -> connection.query(sql, params,
                new ReleasingResultHandler(connection, onResult),
                new ReleasingErrorHandler(connection, onError)), new ChainedErrorHandler(onError));
    }

    @Override
    public void begin(final TransactionHandler onTransaction, final ErrorHandler onError) {
        getConnection(connection -> connection.begin(new TransactionHandler() {
            @Override
            public void onBegin(Transaction transaction) {
                onTransaction.onBegin(new ReleasingTransaction(connection, transaction));
            }
        }, new ReleasingErrorHandler(connection, onError)), new ChainedErrorHandler(onError));
    }

    @Override
    public void close() {
        closed = true;
        synchronized (lock) {
            for(Connection conn = connections.poll(); conn != null; conn = connections.poll()) {
                conn.close();
            }
            for(QueuedCallback waiter = waiters.poll(); waiter != null; waiter = waiters.poll()) {
                waiter.errorHandler.onError(new SqlException("Connection pool is closed"));
            }
        }
    }

    @Override
    public void getConnection(final ConnectionHandler onConnection, final ErrorHandler onError) {
        if(closed) {
            onError.onError(new SqlException("Connection pool is closed"));
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
            onConnection.onConnection(connection);
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
                next.connectionHandler.onConnection(connection);
            }
        }
    }

    /**
     * Creates a new connection to the backend.
     * 
     * @param address
     * @return Unconnected connection
     */
    protected abstract PgConnection newConnection(InetSocketAddress address);


    /**
     * {@link ResultHandler} that releases the connection before calling callback.
     */
    class ReleasingResultHandler implements ResultHandler {
        final Connection conn;
        final ResultHandler onResult;

        ReleasingResultHandler(Connection conn, ResultHandler onResult) {
            this.conn = conn;
            this.onResult = onResult;
        }

        @Override
        public void onResult(ResultSet result) {
            release(conn);
            onResult.onResult(result);
        }
    }

    /**
     * {@link ErrorHandler} that releases the connection before calling callback.
     */
    class ReleasingErrorHandler implements ErrorHandler {
        final Connection conn;
        final ErrorHandler onError;

        ReleasingErrorHandler(Connection conn, ErrorHandler onError) {
            this.conn = conn;
            this.onError = onError;
        }

        @Override
        public void onError(Throwable t) {
            release(conn);
            onError.onError(t);
        }
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
        public void rollback(final TransactionCompletedHandler onCompleted, ErrorHandler rollbackError) {
            transaction.rollback(
                    new ReleasingTransactionCompletedHandler(txconn, onCompleted),
                    new ReleasingErrorHandler(txconn, rollbackError));
        }

        @Override
        public void commit(final TransactionCompletedHandler onCompleted, ErrorHandler commitError) {
            transaction.commit(
                    new ReleasingTransactionCompletedHandler(txconn, onCompleted),
                    new ReleasingErrorHandler(txconn, commitError));
        }

        @Override
        @SuppressWarnings("rawtypes")
        public void query(String sql, List params, ResultHandler onResult, final ErrorHandler onError) {
            if(txconn == null) {
                onError.onError(new SqlException("Transaction is rolled back"));
                return;
            }
            txconn.query(sql, params, onResult, new ErrorHandler() {
                @Override
                public void onError(final Throwable t) {
                    transaction.rollback(new TransactionCompletedHandler() {
                        @Override
                        public void onComplete() {
                            release(txconn);
                            txconn = null;
                            onError.onError(t);
                        }
                    }, new ReleasingErrorHandler(txconn, onError));
                }
            });
        }

        @Override
        public void query(String sql, ResultHandler onResult, final ErrorHandler onError) {
            query(sql, null, onResult, onError);
        }
    }

    /**
     * {@link TransactionCompletedHandler} that releases the connection before calling callback.
     */
    class ReleasingTransactionCompletedHandler implements TransactionCompletedHandler {
        final Connection conn;
        final TransactionCompletedHandler onComplete;

        ReleasingTransactionCompletedHandler(Connection conn, TransactionCompletedHandler onComplete) {
            this.conn = conn;
            this.onComplete = onComplete;
        }

        @Override
        public void onComplete() {
            release(conn);
            onComplete.onComplete();
        }
    }
}
