package com.github.pgasync;

import rx.Observable;

import java.util.function.Consumer;

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

    /**
     * Begins a transaction.
     *
     * @param onTransaction Called when transaction is successfully started.
     * @param onError Called on exception thrown
     */
    default void begin(Consumer<Transaction> onTransaction, Consumer<Throwable> onError) {
        begin().subscribe(onTransaction::accept, onError::accept);
    }
}
