package com.github.pgasync;

import rx.Observable;

import java.util.List;
import java.util.function.Consumer;

/**
 * QueryExecutor submits SQL for execution.
 *
 * @author Antti Laisi
 */
public interface QueryExecutor {

    /**
     * Executes an anonymous prepared statement. Uses native PostgreSQL syntax with $arg instead of ?
     * to mark parameters. Supported parameter types are String, Character, Number, Time, Date, Timestamp
     * and byte[].
     *
     * @param sql SQL to execute
     * @param params Parameter values
     * @return Cold observable that emits 0-n rows.
     */
    Observable<Row> queryRows(String sql, Object... params);

    /**
     * Executes an anonymous prepared statement. Uses native PostgreSQL syntax with $arg instead of ?
     * to mark parameters. Supported parameter types are String, Character, Number, Time, Date, Timestamp
     * and byte[].
     *
     * @param sql SQL to execute
     * @param params Parameter values
     * @return Cold observable that emits a single result set.
     */
    Observable<ResultSet> querySet(String sql, Object... params);

    /**
     * Executes a simple query.
     *
     * @param sql SQL to execute.
     * @param onResult Called when query is completed
     * @param onError Called on exception thrown
     */
    default void query(String sql, Consumer<ResultSet> onResult, Consumer<Throwable> onError) {
        query(sql, null, onResult, onError);
    }

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
    default void query(String sql, List/*<Object>*/ params, Consumer<ResultSet> onResult, Consumer<Throwable> onError) {
        querySet(sql, params != null ? params.toArray() : null).subscribe(onResult::accept, onError::accept);
    }
}
