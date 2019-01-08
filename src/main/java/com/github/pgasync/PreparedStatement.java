package com.github.pgasync;

import com.github.pgasync.impl.PgColumn;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Prepared statement in terms of Postgres.
 * It lives during database session. It should be reused multiple times and it should be closed after using.
 * Doesn't support function call feature because of its deprecation See <a href="https://www.postgresql.org/docs/11/protocol-flow.html#id-1.10.5.7.6"/>.
 */
public interface PreparedStatement {

    /**
     * Fetches the whole row set and returns a {@link CompletableFuture} with an instance of {@link ResultSet}.
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
     * @param processor {@link Consumer} of single data row. Performs some processing of data.
     * @param params Array of query parameters values.
     * @return CompletableFuture that completes when the whole process ends or when an error occurs. Future's value will indicate the number of affected rows by the query.
     */
    CompletableFuture<Integer> fetch(Consumer<Map<String, PgColumn>> onColumns, Consumer<Row> processor, Object... params);

    CompletableFuture<Void> close();

}
