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

import javax.annotation.concurrent.GuardedBy;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Pool for backend connections.
 *
 * @author Antti Laisi
 */
public abstract class PgConnectionPool implements ConnectionPool {

    private class PooledPgConnection implements Connection {

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

        void shutdown() {
            statements.forEach((sql, stmt) -> stmt.delegate.close().join());
            statements.clear();
        }

        @Override
        public CompletableFuture<Void> close() {
            release(this);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Listening> subscribe(String channel, Consumer<String> onNotification) {
            return delegate.subscribe(channel, onNotification);
        }

        @Override
        public CompletableFuture<Transaction> begin() {
            return delegate.begin();
        }

        @Override
        public CompletableFuture<Void> script(Consumer<Map<String, PgColumn>> onColumns, Consumer<Row> onRow, Consumer<Integer> onAffected, String sql) {
            return delegate.script(onColumns, onRow, onAffected, sql);
        }

        @Override
        public CompletableFuture<Integer> query(Consumer<Map<String, PgColumn>> onColumns, Consumer<Row> onRow, String sql, Object... params) {
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

            public PooledPgPreparedStatement(String sql, PgConnection.PgPreparedStatement delegate) {
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
            public CompletableFuture<Integer> fetch(Consumer<Map<String, PgColumn>> onColumns, Consumer<Row> processor, Object... params) {
                return delegate.fetch(onColumns, processor, params);
            }
        }
    }

    private final int maxConnections;
    private final int maxStatements;
    private final ReentrantLock lock = new ReentrantLock();
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

    public PgConnectionPool(PoolProperties properties) {
        this.address = InetSocketAddress.createUnresolved(properties.getHostname(), properties.getPort());
        this.username = properties.getUsername();
        this.password = properties.getPassword();
        this.database = properties.getDatabase();
        this.maxConnections = properties.getMaxConnections();
        this.maxStatements = properties.getMaxStatements();
        this.dataConverter = properties.getDataConverter();
    }

    @Override
    public CompletableFuture<Void> close() {
        lock.lock();
        try {
            closed = true;
            while (!subscribers.isEmpty()) {
                CompletableFuture<? super Connection> queued = subscribers.poll();
                queued.completeExceptionally(new SqlException("Connection pool is closing"));
            }
            while (!connections.isEmpty()) {
                PooledPgConnection connection = connections.poll();
                connection.shutdown();
                size--;
            }
        } finally {
            lock.unlock();
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Connection> getConnection() {
        CompletableFuture<Connection> uponAvailable = new CompletableFuture<>();

        lock.lock();
        try {
            if (closed) {
                uponAvailable.completeExceptionally(new SqlException("Connection pool is closed"));
            } else {
                Connection connection = connections.poll();
                if (connection != null) {
                    uponAvailable.complete(connection);
                } else {
                    if (tryIncreaseSize()) {
                        new PooledPgConnection(new PgConnection(openStream(address), dataConverter))
                                .connect(username, password, database)
                                .thenAccept(uponAvailable::complete)
                                .exceptionally(th -> {
                                    uponAvailable.completeExceptionally(th);
                                    return null;
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

    void release(PooledPgConnection connection) {
        if (connection == null) {
            throw new IllegalArgumentException("'connection' should be non null to be returned to the pool");
        }
        lock.lock();
        try {
            if (closed) {
                if (connection.isConnected()) {
                    connection.shutdown();
                }
            } else {
                if (connection.isConnected()) {
                    if (!subscribers.isEmpty()) {
                        subscribers.poll().complete(connection);
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
    }

    @Override
    public CompletableFuture<Transaction> begin() {
        return getConnection()
                .thenApply(Connection::begin)
                .thenCompose(Function.identity());
    }

    @Override
    public CompletableFuture<Void> script(Consumer<Map<String, PgColumn>> onColumns, Consumer<Row> onRow, Consumer<Integer> onAffected, String sql) {
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
    public CompletableFuture<Integer> query(Consumer<Map<String, PgColumn>> onColumns, Consumer<Row> onRow, String sql, Object... params) {
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
