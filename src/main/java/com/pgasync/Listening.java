package com.pgasync;

import java.util.concurrent.CompletableFuture;

@FunctionalInterface
public interface Listening {

    CompletableFuture<Void> unlisten();
}
