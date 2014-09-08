package com.github.pgasync;

import com.github.pgasync.callback.ErrorHandler;
import com.github.pgasync.callback.TransactionHandler;

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
    void begin(TransactionHandler onTransaction, ErrorHandler onError);

}
