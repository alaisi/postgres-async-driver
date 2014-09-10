package com.github.pgasync;

import java.util.List;
import java.util.function.Consumer;

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
    void query(String sql, Consumer<ResultSet> onResult, Consumer<Throwable> onError);

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
    void query(String sql, List/*<Object>*/ params, Consumer<ResultSet> onResult, Consumer<Throwable> onError);

}
