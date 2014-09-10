package com.github.pgasync;

import java.util.function.Consumer;

/**
 * TransactionExecutor begins backend transactions.
 *
 * @author Antti Laisi
 */
public interface TransactionExecutor {

    /**
     * Begins a transaction.
     *
     * @param onTransaction Called when transaction is successfully started.
     * @param onError Called on exception thrown
     */
    void begin(Consumer<Transaction> onTransaction, Consumer<Throwable> onError);

}
