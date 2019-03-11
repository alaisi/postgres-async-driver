package com.github.pgasync;

import com.pgasync.Connection;
import com.pgasync.NettyConnectibleBuilder;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

public class PgDatabase extends PgConnectible {

    public PgDatabase(NettyConnectibleBuilder.ConnectibleProperties properties, Function<Executor, ProtocolStream> toStream, Executor futuresExecutor) {
        super(properties, toStream, futuresExecutor);
    }

    @Override
    public CompletableFuture<Connection> getConnection() {
        return new PgConnection(toStream.apply(futuresExecutor), dataConverter, encoding)
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
