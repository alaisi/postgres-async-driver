package com.github.pgasync;

import rx.Observable;

/**
 * Main interface to PostgreSQL backend.
 *
 * @author Antti Laisi
 */
public interface Db extends QueryExecutor, TransactionExecutor, Listenable {

    /**
     * Closes the pool.
     */
    Observable<Void> close();

}
