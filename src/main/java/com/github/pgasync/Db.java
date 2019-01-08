package com.github.pgasync;

import java.util.concurrent.CompletableFuture;

/**
 * Main interface to Postgres backend.
 *
 * @author Antti Laisi
 */
public interface Db extends QueryExecutor {

    CompletableFuture<Void> close();
}
