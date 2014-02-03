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
 * @author Antti Laisi
 */
public abstract class PgConnectionPool implements ConnectionPool {

    final Queue<ConnectionHandler> waiters = new LinkedList<>();
    final Queue<Connection> connections = new LinkedList<>();

    final InetSocketAddress address;
    final String username;
    final String password;
    final String database;

    final int poolSize;
    int currentSize;

    public PgConnectionPool(InetSocketAddress address, String username, String password, String database,
            int poolSize) {
        this.address = address;
        this.username = username;
        this.password = password;
        this.database = database;
        this.poolSize = poolSize;
    }

    @Override
    public void query(final String sql, final ResultHandler onResult, final ErrorHandler onError) {
        query(sql, null, onResult, onError);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void query(final String sql, final List params, final ResultHandler onResult, final ErrorHandler onError) {
        getConnection(new ConnectionHandler() {
            @Override
            public void onConnection(Connection connection) {
                connection.query(sql, params, 
                        new ReleasingResultHandler(connection, onResult),
                        new ReleasingErrorHandler(connection, onError));
            }
        }, new ChainedErrorHandler(onError));
    }

    @Override
    public void begin(final TransactionHandler onTransaction, final ErrorHandler onError) {
        getConnection(new ConnectionHandler() {
            @Override
            public void onConnection(Connection connection) {
                connection.begin(new TransactionHandler() {
                    @Override
                    public void onBegin(final Connection txconn, Transaction transaction) {
                        Transaction releasingTx = new ReleasingTransaction(txconn, transaction);
                        onTransaction.onBegin(new TransactionalConnection(txconn, releasingTx), releasingTx);
                    }
                }, new ReleasingErrorHandler(connection, onError));
            }
        }, new ChainedErrorHandler(onError));
    }

    @Override
    public void close() {
        // TODO
    }

    @Override
    public void getConnection(final ConnectionHandler onConnection, final ErrorHandler onError) {
        Connection connection;
        boolean create = false;
        synchronized (this) {
            connection = connections.poll();
            if (connection == null) {
                if (currentSize < poolSize) {
                    create = true;
                    currentSize++;
                } else {
                    waiters.add(onConnection);
                    return;
                }
            }
        }
        if (connection != null) {
            onConnection.onConnection(connection);
        } else if (create) {
            newConnection(address).connect(username, password, database, onConnection, onError);
        }
    }

    /**
     * Releases a connection back to the pool.
     */
    void release(Connection connection) {
        ConnectionHandler waiter;
        synchronized (this) {
            if(!((PgConnection) connection).isConnected()) {
                currentSize--;
                return; // TODO: stale pool?
            }
            waiter = waiters.poll();
            if (waiter == null) {
                connections.add(connection);
            }
        }
        if (waiter != null) {
            waiter.onConnection(connection);
        }
    }

    /**
     * Creates a new connection to the backend.
     * 
     * @param address
     * @return Unconnection connection
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
     * {@link Connection} that prevents transaction nesting and rollbacks the transaction
     * on backend error.
     */
    class TransactionalConnection implements Connection {
        Connection txconn;
        Transaction transaction;

        TransactionalConnection(Connection txconn, Transaction transaction) {
            this.txconn = txconn;
            this.transaction = transaction;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public void query(String sql, List params, ResultHandler onResult, final ErrorHandler onError) {
            txconn.query(sql, params, onResult, new ErrorHandler() {
                @Override
                public void onError(final Throwable t) {
                    transaction.rollback(new TransactionCompletedHandler() {
                        @Override
                        public void onComplete() {
                            onError.onError(t);
                        }
                    }, onError);
                }
            });
        }

        @Override
        public void query(String sql, ResultHandler onResult, final ErrorHandler onError) {
            query(sql, null, onResult, onError);
        }

        @Override
        public void begin(TransactionHandler onTransaction, ErrorHandler onError) {
            throw new SqlException("Nested transactions are not supported");
        }

        @Override
        public void close() {
            txconn.close();
        }
    }

    /**
     * Transaction that chains releasing the connection after COMMIT/ROLLBACK.
     */
    class ReleasingTransaction implements Transaction {
        final Connection txconn;
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
