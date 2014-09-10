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

import static com.github.pgasync.impl.message.ErrorResponse.Level.FATAL;

import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.pgasync.*;
import com.github.pgasync.callback.ConnectionHandler;
import com.github.pgasync.callback.ErrorHandler;
import com.github.pgasync.callback.ResultHandler;
import com.github.pgasync.callback.TransactionCompletedHandler;
import com.github.pgasync.callback.TransactionHandler;
import com.github.pgasync.impl.message.Authentication;
import com.github.pgasync.impl.message.Bind;
import com.github.pgasync.impl.message.CommandComplete;
import com.github.pgasync.impl.message.DataRow;
import com.github.pgasync.impl.message.ErrorResponse;
import com.github.pgasync.impl.message.ExtendedQuery;
import com.github.pgasync.impl.message.Parse;
import com.github.pgasync.impl.message.PasswordMessage;
import com.github.pgasync.impl.message.Query;
import com.github.pgasync.impl.message.ReadyForQuery;
import com.github.pgasync.impl.message.RowDescription;
import com.github.pgasync.impl.message.StartupMessage;

/**
 * A connection to PostgreSQL backed. The postmaster forks a backend process for
 * each connection. A connection can process only a single query at a time.
 * 
 * @author Antti Laisi
 */
public class PgConnection implements Connection, PgProtocolCallbacks {

    final PgProtocolStream stream;
    final ConverterRegistry converterRegistry;

    String username;
    String password;

    // errorHandler must be read before and be written after queryHandler/connectedHandler (memory barrier)
    volatile ErrorHandler errorHandler;
    ConnectionHandler connectedHandler;
    ResultHandler queryHandler; 

    volatile boolean connected;

    PgResultSet resultSet;

    public PgConnection(PgProtocolStream stream, ConverterRegistry converterRegistry) {
        this.stream = stream;
        this.converterRegistry = converterRegistry;
    }

    public void connect(String username, String password, String database, 
            ConnectionHandler onConnected, ErrorHandler onError) {
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
    public void query(String sql, ResultHandler onQuery, ErrorHandler onError) {
        if (queryHandler != null) {
            onError.onError(new IllegalStateException("Query already in progress"));
            return;
        }
        queryHandler = onQuery;
        errorHandler = onError;
        stream.send(new Query(sql));
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void query(String sql, List params, ResultHandler onQuery, ErrorHandler onError) {
        if (params == null || params.isEmpty()) {
            query(sql, onQuery, onError);
            return;
        }
        if (queryHandler != null) {
            onError.onError(new IllegalStateException("Query already in progress"));
            return;
        }
        queryHandler = onQuery;
        errorHandler = onError;
        stream.send(
                new Parse(sql), 
                new Bind(TypeConverter.toBackendParams(params, converterRegistry)),
                ExtendedQuery.DESCRIBE, 
                ExtendedQuery.EXECUTE,
                ExtendedQuery.CLOSE,
                ExtendedQuery.SYNC);
    }

    @Override
    public void begin(final TransactionHandler handler, final ErrorHandler onError) {

        final TransactionCompletedHandler[] onCompletedRef = new TransactionCompletedHandler[1];
        final ResultHandler queryToComplete = rows -> onCompletedRef[0].onComplete();

        final Transaction transaction = new ConnectionTx() {
            @Override
            public void commit(TransactionCompletedHandler onCompleted, ErrorHandler onCommitError) {
                onCompletedRef[0] = onCompleted;
                query("COMMIT", queryToComplete, onCommitError);
            }
            @Override
            public void rollback(TransactionCompletedHandler onCompleted, ErrorHandler onRollbackError) {
                onCompletedRef[0] = onCompleted;
                query("ROLLBACK", queryToComplete, onRollbackError);
            }
        };

        query("BEGIN", ignored -> {
            try {
                handler.onBegin(transaction);
            } catch (Exception e) {
                invokeOnError(onError, e);
            }
        }, onError);
    }

    // PgProtocolCallbacks

    @Override
    public void onThrowable(Throwable t) {
        ErrorHandler err = errorHandler;

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
        onThrowable(new SqlException(msg.getLevel().toString(), msg.getCode(), msg.getMessage()));
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
        resultSet.add(new PgRow(msg, converterRegistry));
    }

    @Override
    public void onReadyForQuery(ReadyForQuery msg) {
        ErrorHandler onError = errorHandler;
        if (!connected) {
            onConnected();
            return;
        }
        if (queryHandler != null) {
            ResultHandler onResult = queryHandler;
            ResultSet result = resultSet;

            queryHandler = null;
            resultSet = null;
            errorHandler = null;

            try {
                onResult.onResult(result);
            } catch (Exception e) {
                invokeOnError(onError, e);
            }
        }
    }

    void onConnected() {
        connected = true;
        try {
            connectedHandler.onConnection(this);
        } catch (Exception e) {
            invokeOnError(errorHandler, e);
        }
        connectedHandler = null;
    }

    void invokeOnError(ErrorHandler err, Throwable t) {
        if (err != null) {
            try {
                err.onError(t);
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
        public void query(String sql, ResultHandler onResult, ErrorHandler onError) {
            PgConnection.this.query(sql, onResult, onError);
        }
        @Override
        public void query(String sql, List params, ResultHandler onResult, ErrorHandler onError) {
            PgConnection.this.query(sql, params, onResult, onError);
        }
    }
}
