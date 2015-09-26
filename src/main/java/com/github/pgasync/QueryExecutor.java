package com.github.pgasync;

import rx.Observable;

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
     */
    Observable<Row> queryRows(String sql, Object... params);

    /**
     * Executes a simple queryRows.
     *
     * @param sql SQL to execute.
     */
    default Observable<Row> queryRows(String sql) {
        return queryRows(sql, (Object[]) null);
    }

    Observable<ResultSet> querySet(String sql, Object... params);

    default Observable<ResultSet> querySet(String sql) {
        return querySet(sql, (Object[]) null);
    }

}
