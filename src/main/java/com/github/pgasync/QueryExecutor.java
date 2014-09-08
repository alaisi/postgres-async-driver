package com.github.pgasync;

import com.github.pgasync.callback.ErrorHandler;
import com.github.pgasync.callback.ResultHandler;

import java.util.List;

/**
 * QueryExecutor submits SQL for execution.
 *
 * @author Antti Laisi
 */
public interface QueryExecutor {

    /**
     * Executes a simple query.
     *
     * @param sql SQL to execute.
     * @param onResult Called when query is completed
     * @param onError Called on exception thrown
     */
    void query(String sql, ResultHandler onResult, ErrorHandler onError);

    /**
     * Executes an anonymous prepared statement. Uses native PostgreSQL syntax with $arg instead of ?
     * to mark parameters. Supported parameter types are String, Character, Number, Time, Date, Timestamp
     * and byte[].
     *
     * @param sql SQL to execute
     * @param params List of parameters
     * @param onResult Called when query is completed
     * @param onError Called on exception thrown
     */
    void query(String sql, List/*<Object>*/ params, ResultHandler onResult, ErrorHandler onError);

}
