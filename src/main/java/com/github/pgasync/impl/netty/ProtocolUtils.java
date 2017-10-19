package com.github.pgasync.impl.netty;

import com.github.pgasync.SqlException;
import com.github.pgasync.impl.message.Authentication;
import com.github.pgasync.impl.message.ErrorResponse;
import com.github.pgasync.impl.message.Message;
import com.github.pgasync.impl.message.ReadyForQuery;

import java.util.Optional;

class ProtocolUtils {
    static Optional<SqlException> asSqlException(Message message) {
        if (isErrorMessage(message))
            return Optional.of(toSqlException((ErrorResponse) message));
        else
            return Optional.empty();
    }

    static SqlException toSqlException(ErrorResponse error) {
        return new SqlException(error.getLevel().name(), error.getCode(), error.getMessage());
    }

    static boolean isCompleteMessage(Message msg) {
        return msg == ReadyForQuery.INSTANCE
                || (msg instanceof Authentication && !((Authentication) msg).isAuthenticationOk());
    }

    static boolean isErrorMessage(Message msg) {
        return msg instanceof ErrorResponse;
    }
}
