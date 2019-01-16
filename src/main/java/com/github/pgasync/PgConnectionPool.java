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

package com.github.pgasync;

import com.github.pgasync.conversion.DataConverter;
import com.pgasync.Connection;
import com.pgasync.Listening;
import com.pgasync.PreparedStatement;
import com.pgasync.Row;
import com.pgasync.SqlException;
import com.pgasync.ConnectionPool;
import com.pgasync.ConnectionPoolBuilder;
import com.pgasync.ResultSet;
import com.pgasync.Transaction;

import javax.annotation.concurrent.GuardedBy;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Pool for backend connections.
 *
 * @author Antti Laisi
 */
public abstract class PgConnectionPool implements ConnectionPool {

    private class PooledPgConnection implements Connection {

        private class PooledPgTransaction implements Transaction {

            private final Transaction delegate;

            PooledPgTransaction(Transaction delegate) {
                this.delegate = delegate;
            }

            public CompletableFuture<Void> commit() {
                return delegate.commit();
            }

            public CompletableFuture<Void> rollback() {
                return delegate.rollback();
            }

            public CompletableFuture<Void> close() {
                return delegate.close();
            }

            public CompletableFuture<Transaction> begin() {
                return delegate.begin().thenApply(PooledPgTransaction::new);
            }

            @Override
            public CompletableFuture<Integer> query(BiConsumer<Map<String, PgColumn>, PgColumn[]> onColumns, Consumer<Row> onRow, String sql, Object... params) {
                return delegate.query(onColumns, onRow, sql, params);
            }

            @Override
            public CompletableFuture<Void> script(BiConsumer<Map<String, PgColumn>, PgColumn[]> onColumns, Consumer<Row> onRow, Consumer<Integer> onAffected, String sql) {
                return delegate.script(onColumns, onRow, onAffected, sql);
            }

            public Connection getConnection() {
                return PooledPgConnection.this;
            }
        }

        private final PgConnection delegate;
        private PooledPgPreparedStatement evicted;
        private final LinkedHashMap<String, PooledPgPreparedStatement> statements = new LinkedHashMap<>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, PooledPgPreparedStatement> eldest) {
                if (size() > maxStatements) {
                    evicted = eldest.getValue();
                    return true;
                } else {
                    return false;
                }
            }
        };

        PooledPgConnection(PgConnection delegate) {
            this.delegate = delegate;
        }

        CompletableFuture<Connection> connect(String username, String password, String database) {
            return delegate.connect(username, password, database).thenApply(conn -> PooledPgConnection.this);
        }

        boolean isConnected() {
            return delegate.isConnected();
        }

        CompletableFuture<Void> shutdown() {
            CompletableFuture<?>[] closeTasks = statements.values().stream()
                    .map(stmt -> stmt.delegate.close())
                    .collect(Collectors.toList()).toArray(new CompletableFuture<?>[]{});
            statements.clear();
            return CompletableFuture.allOf(closeTasks)
                    .thenApply(v -> {
                        if (!statements.isEmpty()) {
                            Logger.getLogger(PooledPgConnection.class.getName()).log(Level.WARNING, "Stale prepared statements detected {0}", statements.size());
                        }
                        return delegate.close();
                    })
                    .thenCompose(Function.identity());
        }

        @Override
        public CompletableFuture<Void> close() {
            return release(this);
        }

        @Override
        public CompletableFuture<Listening> subscribe(String channel, Consumer<String> onNotification) {
            return delegate.subscribe(channel, onNotification);
        }

        @Override
        public CompletableFuture<Transaction> begin() {
            return delegate.begin().thenApply(PooledPgTransaction::new);
        }

        @Override
        public CompletableFuture<Void> script(BiConsumer<Map<String, PgColumn>, PgColumn[]> onColumns, Consumer<Row> onRow, Consumer<Integer> onAffected, String sql) {
            return delegate.script(onColumns, onRow, onAffected, sql);
        }

        @Override
        public CompletableFuture<Integer> query(BiConsumer<Map<String, PgColumn>, PgColumn[]> onColumns, Consumer<Row> onRow, String sql, Object... params) {
            return delegate.query(onColumns, onRow, sql, params);
        }

        @Override
        public CompletableFuture<PreparedStatement> prepareStatement(String sql, Oid... parametersTypes) {
            PooledPgPreparedStatement statement = statements.remove(sql);
            if (statement != null) {
                return CompletableFuture.completedFuture(statement);
            } else {
                return delegate.preparedStatementOf(sql, parametersTypes)
                        .thenApply(stmt -> new PooledPgPreparedStatement(sql, stmt));
            }
        }

        private class PooledPgPreparedStatement implements PreparedStatement {

            private final String sql;
            private final PgConnection.PgPreparedStatement delegate;

            private PooledPgPreparedStatement(String sql, PgConnection.PgPreparedStatement delegate) {
                this.sql = sql;
                this.delegate = delegate;
            }

            @Override
            public CompletableFuture<Void> close() {
                statements.put(sql, this);
                if (evicted != null) {
                    try {
                        return evicted.delegate.close();
                    } finally {
                        evicted = null;
                    }
                } else {
                    return CompletableFuture.completedFuture(null);
                }
            }

            @Override
            public CompletableFuture<ResultSet> query(Object... params) {
                return delegate.query(params);
            }

            @Override
            public CompletableFuture<Integer> fetch(BiConsumer<Map<String, PgColumn>, PgColumn[]> onColumns, Consumer<Row> processor, Object... params) {
                return delegate.fetch(onColumns, processor, params);
            }
        }
    }

    private final int maxConnections;
    private final int maxStatements;
    private final String validationQuery;
    private final ReentrantLock lock = new ReentrantLock();
    protected final Charset encoding;

    @GuardedBy("lock")
    private int size;
    @GuardedBy("lock")
    private boolean closed;
    @GuardedBy("lock")
    private final Queue<CompletableFuture<? super Connection>> subscribers = new LinkedList<>();
    @GuardedBy("lock")
    private final Queue<PooledPgConnection> connections = new LinkedList<>();

    private final InetSocketAddress address;
    private final String username;
    private final String password;
    private final String database;
    private final DataConverter dataConverter;

    protected final Executor futuresExecutor;

    public PgConnectionPool(ConnectionPoolBuilder.PoolProperties properties, Executor futuresExecutor) {
        this.address = InetSocketAddress.createUnresolved(properties.getHostname(), properties.getPort());
        this.username = properties.getUsername();
        this.password = properties.getPassword();
        this.database = properties.getDatabase();
        this.maxConnections = properties.getMaxConnections();
        this.maxStatements = properties.getMaxStatements();
        this.dataConverter = properties.getDataConverter();
        this.validationQuery = properties.getValidationQuery();
        this.encoding = Charset.forName(properties.getEncoding());
        this.futuresExecutor = futuresExecutor;
    }

    @Override
    public CompletableFuture<Void> close() {
        Collection<CompletableFuture<Void>> shutdownTasks = new ArrayList<>();
        lock.lock();
        try {
            closed = true;
            while (!subscribers.isEmpty()) {
                CompletableFuture<? super Connection> queued = subscribers.poll();
                futuresExecutor.execute(() -> queued.completeExceptionally(new SqlException("Connection pool is closing")));
            }
            while (!connections.isEmpty()) {
                PooledPgConnection connection = connections.poll();
                shutdownTasks.add(connection.shutdown());
                size--;
            }
        } finally {
            lock.unlock();
        }
        return CompletableFuture.allOf(shutdownTasks.toArray(size -> new CompletableFuture<?>[size]));
    }

    @Override
    public CompletableFuture<Connection> getConnection() {
        CompletableFuture<Connection> uponAvailable = new CompletableFuture<>();

        lock.lock();
        try {
            if (closed) {
                futuresExecutor.execute(() -> uponAvailable.completeExceptionally(new SqlException("Connection pool is closed")));
            } else {
                Connection connection = connections.poll();
                if (connection != null) {
                    uponAvailable.completeAsync(() -> connection, futuresExecutor);
                } else {
                    if (tryIncreaseSize()) {
                        new PooledPgConnection(new PgConnection(openStream(address), dataConverter, encoding))
                                .connect(username, password, database)
                                .thenApply(pooledConnection -> {
                                    if (validationQuery != null && !validationQuery.isBlank()) {
                                        return pooledConnection.completeQuery(validationQuery).thenApply(rs -> pooledConnection);
                                    } else {
                                        return CompletableFuture.completedFuture(pooledConnection);
                                    }
                                })
                                .thenCompose(Function.identity())
                                .thenAccept(pooledConnection -> uponAvailable.completeAsync(() -> pooledConnection, futuresExecutor))
                                .exceptionally(th -> {
                                    lock.lock();
                                    try {
                                        size--;
                                        futuresExecutor.execute(() -> uponAvailable.completeExceptionally(th));
                                        return null;
                                    } finally {
                                        lock.unlock();
                                    }
                                });
                    } else {
                        // Pool is full now and all connections are busy
                        subscribers.offer(uponAvailable);
                    }
                }
            }
        } finally {
            lock.unlock();
        }

        return uponAvailable;
    }

    private boolean tryIncreaseSize() {
        if (size < maxConnections) {
            size++;
            return true;
        } else {
            return false;
        }
    }

    private CompletableFuture<Void> release(PooledPgConnection connection) {
        if (connection == null) {
            throw new IllegalArgumentException("'connection' should be not null");
        }
        CompletableFuture<Void> shutdownTask = CompletableFuture.completedFuture(null);
        lock.lock();
        try {
            if (closed) {
                if (connection.isConnected()) {
                    shutdownTask = connection.shutdown();
                }
            } else {
                if (connection.isConnected()) {
                    if (!subscribers.isEmpty()) {
                        subscribers.poll().completeAsync(() -> connection, futuresExecutor);
                    } else {
                        connections.offer(connection);
                    }
                } else {
                    size--;
                }
            }
        } finally {
            lock.unlock();
        }
        return shutdownTask;
    }

    @Override
    public CompletableFuture<Transaction> begin() {
        return getConnection()
                .thenApply(Connection::begin)
                .thenCompose(Function.identity());
    }

    @Override
    public CompletableFuture<Void> script(BiConsumer<Map<String, PgColumn>, PgColumn[]> onColumns, Consumer<Row> onRow, Consumer<Integer> onAffected, String sql) {
        return getConnection()
                .thenApply(connection ->
                        connection.script(onColumns, onRow, onAffected, sql)
                                .handle((message, th) ->
                                        connection.close()
                                                .thenApply(v -> {
                                                    if (th == null) {
                                                        return message;
                                                    } else {
                                                        throw new RuntimeException(th);
                                                    }
                                                })
                                ).thenCompose(Function.identity())
                )
                .thenCompose(Function.identity());
    }

    @Override
    public CompletableFuture<Integer> query(BiConsumer<Map<String, PgColumn>, PgColumn[]> onColumns, Consumer<Row> onRow, String sql, Object... params) {
        return getConnection()
                .thenApply(connection ->
                        connection.query(onColumns, onRow, sql, params)
                                .handle((affected, th) ->
                                        connection.close()
                                                .thenApply(v -> {
                                                    if (th == null) {
                                                        return affected;
                                                    } else {
                                                        throw new RuntimeException(th);
                                                    }
                                                })
                                ).thenCompose(Function.identity())
                )
                .thenCompose(Function.identity());
    }

    /**
     * Creates a new socket stream to the backend.
     *
     * @param address Server address
     * @return Stream with no pending messages
     */
    protected abstract PgProtocolStream openStream(InetSocketAddress address);
}
