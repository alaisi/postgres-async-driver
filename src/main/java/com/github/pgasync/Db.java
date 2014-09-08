package com.github.pgasync;

/**
 * Main interface to PostgreSQL backend.
 *
 * @author Antti Laisi
 */
public interface Db extends QueryExecutor, TransactionExecutor {

}
