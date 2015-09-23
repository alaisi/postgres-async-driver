package com.github.pgasync;

import rx.Observable;

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
     */
    Observable<Row> query(String sql);

    /**
     * Executes an anonymous prepared statement. Uses native PostgreSQL syntax with $arg instead of ?
     * to mark parameters. Supported parameter types are String, Character, Number, Time, Date, Timestamp
     * and byte[].
     *
     * @param sql SQL to execute
     * @param params Parameter values
     */
    Observable<Row> query(String sql, Object... params);

}
