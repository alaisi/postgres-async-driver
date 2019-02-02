package com.pgasync;

import com.github.pgasync.PgColumn;
import com.github.pgasync.message.backend.DataRow;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Prepared statement in terms of Postgres.
 * It lives during database session. It should be reused multiple times and it should be closed after using.
 * Doesn't support function call feature because of its deprecation.
 * @see <a href="https://www.postgresql.org/docs/11/protocol-flow.html#id-1.10.5.7.6"/>.
 *
 * Concurrent using of implementations is impossible.
 * {@link PreparedStatement} implementations are never thread-safe.
 * They are designed to be used in context of single {@link CompletableFuture} completion at a time.
 */
public interface PreparedStatement {

    /**
     * Fetches the whole row set and returns a {@link CompletableFuture} completed with an instance of {@link ResultSet}.
     * This future may be completed with an error. Use this method if you are sure, that all data, returned by the query can be placed into memory.
     *
     * @param params Array of query parameters values.
     * @return An instance of {@link ResultSet} with data.
     */
    CompletableFuture<ResultSet> query(Object... params);

    /**
     * Fetches data rows from Postgres one by one. Use this method when you are unsure, that all data, returned by the query can be placed into memory.
     *
     * @param onColumns {@link Consumer} of parameters by name map. Gets called when bind/describe chain succeeded.
     * @param processor {@link Consumer} of single data row. Performs transformation from {@link DataRow} message
     *                                  to {@link Row} implementation instance.
     * @param params Array of query parameters values.
     * @return CompletableFuture that completes when the whole process ends or when an error occurs. Future's value will indicate the number of rows affected by the query.
     */
    CompletableFuture<Integer> fetch(BiConsumer<Map<String, PgColumn>, PgColumn[]> onColumns, Consumer<Row> processor, Object... params);

    /**
     * Closes this {@link PreparedStatement} and possibly frees resources. In case of pool statement it may be returned to a pool for future reuse.
     * @return CompletableFuture that is completed when the network process ends.
     * Network process may occur if returned statement has evicted some other statement from the pool in case of pooled statement.
     * Closing of such evicted statement is network activity, we should be aware of.
     */
    CompletableFuture<Void> close();

}
