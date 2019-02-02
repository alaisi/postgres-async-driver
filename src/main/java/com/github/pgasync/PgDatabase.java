package com.github.pgasync;

import com.pgasync.Connection;
import com.pgasync.ConnectibleBuilder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

public abstract class PgDatabase extends PgConnectible {

    public PgDatabase(ConnectibleBuilder.ConnectibleProperties properties, Executor futuresExecutor) {
        super(properties, futuresExecutor);
    }

    @Override
    public CompletableFuture<Connection> getConnection() {
        return new PgConnection(openStream(address), dataConverter, encoding)
                .connect(username, password, database)
                .thenApply(connection -> {
                    if (validationQuery != null && !validationQuery.isBlank()) {
                        return connection.completeScript(validationQuery)
                                .handle((rss, th) -> {
                                    if (th != null) {
                                        return connection.close()
                                                .thenApply(v -> CompletableFuture.<Connection>failedFuture(th))
                                                .thenCompose(Function.identity());
                                    } else {
                                        return CompletableFuture.completedFuture(connection);
                                    }
                                })
                                .thenCompose(Function.identity());
                    } else {
                        return CompletableFuture.completedFuture(connection);
                    }
                })
                .thenCompose(Function.identity());
    }

    @Override
    public CompletableFuture<Void> close() {
        return CompletableFuture.completedFuture(null);
    }

}
