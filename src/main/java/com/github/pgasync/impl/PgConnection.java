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

import com.github.pgasync.Connection;
import com.github.pgasync.ResultSet;
import com.github.pgasync.SqlException;
import com.github.pgasync.Transaction;
import com.github.pgasync.impl.conversion.DataConverter;
import com.github.pgasync.impl.message.*;

import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.github.pgasync.impl.message.ErrorResponse.Level.FATAL;

/**
 * A connection to PostgreSQL backed. The postmaster forks a backend process for
 * each connection. A connection can process only a single query at a time.
 * 
 * @author Antti Laisi
 */
public class PgConnection implements Connection, PgProtocolCallbacks {

    final PgProtocolStream stream;
    final DataConverter dataConverter;

    String username;
    String password;

    // errorHandler must be read before and be written after queryHandler/connectedHandler (memory barrier)
    volatile Consumer<Throwable> errorHandler;
    Consumer<Connection> connectedHandler;
    Consumer<ResultSet> queryHandler;

    volatile boolean connected;

    PgResultSet resultSet;

    public PgConnection(PgProtocolStream stream, DataConverter dataConverter) {
        this.stream = stream;
        this.dataConverter = dataConverter;
    }

    public void connect(String username, String password, String database,
                        Consumer<Connection> onConnected, Consumer<Throwable> onError) {
        this.username = username;
        this.password = password;
        this.connectedHandler = onConnected;
        this.errorHandler = onError;
        stream.connect(new StartupMessage(username, database), this);
    }

    public boolean isConnected() {
        return connected;
    }

    @Override
    public void close() {
        stream.close();
        connected = false;
    }

    // Connection

    @Override
    public void query(String sql, Consumer<ResultSet> onQuery, Consumer<Throwable> onError) {
        if (queryHandler != null) {
            invokeOnError(onError, new IllegalStateException("Query already in progress"));
            return;
        }
        queryHandler = onQuery;
        errorHandler = onError;
        stream.send(new Query(sql));
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void query(String sql, List params, Consumer<ResultSet> onQuery, Consumer<Throwable> onError) {
        if (params == null || params.isEmpty()) {
            query(sql, onQuery, onError);
            return;
        }
        if (queryHandler != null) {
            invokeOnError(onError, new IllegalStateException("Query already in progress"));
            return;
        }
        queryHandler = onQuery;
        errorHandler = onError;
        stream.send(
                new Parse(sql), 
                new Bind(dataConverter.fromParameters(params)),
                ExtendedQuery.DESCRIBE, 
                ExtendedQuery.EXECUTE,
                ExtendedQuery.CLOSE,
                ExtendedQuery.SYNC);
    }

    @Override
    public void begin(Consumer<Transaction> handler, Consumer<Throwable> onError) {

        Transaction transaction = new ConnectionTx() {
            @Override
            public void commit(Runnable onCompleted, Consumer<Throwable> onCommitError) {
                query("COMMIT", (rs) -> onCompleted.run(), onCommitError);
            }
            @Override
            public void rollback(Runnable onCompleted, Consumer<Throwable> onRollbackError) {
                query("ROLLBACK", (rs) -> onCompleted.run(), onRollbackError);
            }
        };

        query("BEGIN", beginRs -> {
            try {
                handler.accept(transaction);
            } catch (Exception e) {
                invokeOnError(onError, e);
            }
        }, onError);
    }

    // PgProtocolCallbacks

    @Override
    public void onThrowable(Throwable t) {
        Consumer<Throwable> err = errorHandler;

        queryHandler = null;
        resultSet = null;
        errorHandler = null;

        if(t instanceof ClosedChannelException && connected) {
            close();
        }

        invokeOnError(err, t);
    }

    @Override
    public void onErrorResponse(ErrorResponse msg) {
        if(msg.getLevel() == FATAL) {
            close();
        }
        onThrowable(new SqlException(msg.getLevel().name(), msg.getCode(), msg.getMessage()));
    }

    @Override
    public void onAuthentication(Authentication msg) {
        if (!msg.isAuthenticationOk()) {
            stream.send(new PasswordMessage(username, password, msg.getMd5Salt()));
            username = password = null;
        }
    }

    @Override
    public void onRowDescription(RowDescription msg) {
        resultSet = new PgResultSet(msg.getColumns());
    }

    @Override
    public void onCommandComplete(CommandComplete msg) {
        if (resultSet == null) {
            resultSet = new PgResultSet();
        }
        resultSet.setUpdatedRows(msg.getUpdatedRows());
    }

    @Override
    public void onDataRow(DataRow msg) {
        resultSet.add(new PgRow(msg, dataConverter));
    }

    @Override
    public void onReadyForQuery(ReadyForQuery msg) {
        Consumer<Throwable> onError = errorHandler;
        if (!connected) {
            onConnected();
            return;
        }
        if (queryHandler != null) {
            Consumer<ResultSet> onResult = queryHandler;
            ResultSet result = resultSet;

            queryHandler = null;
            resultSet = null;
            errorHandler = null;

            try {
                onResult.accept(result);
            } catch (Exception e) {
                invokeOnError(onError, e);
            }
        }
    }

    void onConnected() {
        connected = true;
        try {
            connectedHandler.accept(this);
        } catch (Exception e) {
            invokeOnError(errorHandler, e);
        }
        connectedHandler = null;
    }

    void invokeOnError(Consumer<Throwable> err, Throwable t) {
        if (err != null) {
            try {
                err.accept(t);
            } catch (Exception e) {
                Logger.getLogger(getClass().getName()).log(Level.SEVERE,
                        "ErrorHandler " + err + " failed with exception", e);
            }
        } else if(!(t instanceof ClosedChannelException)) {
            Logger.getLogger(getClass().getName()).log(Level.SEVERE,
                    "Exception caught but no error handler is set", t);
        }
    }

    abstract class ConnectionTx implements Transaction {
        @Override
        public void query(String sql, Consumer<ResultSet> onResult, Consumer<Throwable> onError) {
            PgConnection.this.query(sql, onResult, onError);
        }
        @Override
        public void query(String sql, List params, Consumer<ResultSet> onResult, Consumer<Throwable> onError) {
            PgConnection.this.query(sql, params, onResult, onError);
        }
    }
}
