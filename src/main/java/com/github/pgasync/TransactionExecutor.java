package com.github.pgasync;

import rx.Observable;

/**
 * TransactionExecutor begins backend transactions.
 *
 * @author Antti Laisi
 */
public interface TransactionExecutor {

    /**
     * Begins a transaction.
     */
    Observable<Transaction> begin();

}
