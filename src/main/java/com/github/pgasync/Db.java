package com.github.pgasync;

/**
 * Main interface to PostgreSQL backend.
 *
 * @author Antti Laisi
 */
public interface Db extends QueryExecutor, TransactionExecutor, Listenable, AutoCloseable {

    /**
     * Closes the pool, blocks the calling thread until connections are closed.
     */
    @Override
    void close() throws Exception;

}
