package com.github.pgasync;

import com.github.pgasync.conversion.DataConverter;
import com.pgasync.Connection;
import com.pgasync.NettyConnectibleBuilder;
import com.pgasync.Connectible;
import com.pgasync.Row;
import com.pgasync.Transaction;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class PgConnectible implements Connectible {

    final String validationQuery;
    final String username;
    final DataConverter dataConverter;
    final Executor futuresExecutor;
    final Function<Executor, ProtocolStream> toStream;

    protected final String password;
    protected final String database;
    protected final Charset encoding;

    PgConnectible(NettyConnectibleBuilder.ConnectibleProperties properties, Function<Executor, ProtocolStream> toStream, Executor futuresExecutor) {
        this.username = properties.getUsername();
        this.password = properties.getPassword();
        this.database = properties.getDatabase();
        this.dataConverter = properties.getDataConverter();
        this.validationQuery = properties.getValidationQuery();
        this.encoding = Charset.forName(properties.getEncoding());
        this.toStream = toStream;
        this.futuresExecutor = futuresExecutor;
    }

    @Override
    public CompletableFuture<Transaction> begin() {
        return getConnection()
                .thenApply(Connection::begin)
                .thenCompose(Function.identity());
    }

    @Override
    public CompletableFuture<Void> script(BiConsumer<Map<String, PgColumn>, PgColumn[]> onColumns, Consumer<Row> onRow, Consumer<Integer> onAffected, String sql) {
        return getConnection()
                .thenApply(connection ->
                        connection.script(onColumns, onRow, onAffected, sql)
                                .handle((message, th) ->
                                        connection.close()
                                                .thenApply(v -> {
                                                    if (th == null) {
                                                        return message;
                                                    } else {
                                                        throw new RuntimeException(th);
                                                    }
                                                })
                                ).thenCompose(Function.identity())
                ).thenCompose(Function.identity());
    }

    @Override
    public CompletableFuture<Integer> query(BiConsumer<Map<String, PgColumn>, PgColumn[]> onColumns, Consumer<Row> onRow, String sql, Object... params) {
        return getConnection()
                .thenApply(connection ->
                        connection.query(onColumns, onRow, sql, params)
                                .handle((affected, th) ->
                                        connection.close()
                                                .thenApply(v -> {
                                                    if (th == null) {
                                                        return affected;
                                                    } else {
                                                        throw new RuntimeException(th);
                                                    }
                                                })
                                ).thenCompose(Function.identity())
                )
                .thenCompose(Function.identity());
    }

}
