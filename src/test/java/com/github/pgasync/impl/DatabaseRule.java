package com.github.pgasync.impl;

import static java.lang.System.getenv;
import static java.lang.System.out;
import static ru.yandex.qatools.embed.postgresql.distribution.Version.*;

import com.github.pgasync.ConnectionPool;
import com.github.pgasync.ConnectionPoolBuilder;
import com.github.pgasync.Db;
import com.github.pgasync.ResultSet;
import com.github.pgasync.SqlException;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import ru.yandex.qatools.embed.postgresql.PostgresExecutable;
import ru.yandex.qatools.embed.postgresql.PostgresProcess;
import ru.yandex.qatools.embed.postgresql.PostgresStarter;
import ru.yandex.qatools.embed.postgresql.config.AbstractPostgresConfig;
import ru.yandex.qatools.embed.postgresql.config.PostgresConfig;

/**
 * @author Antti Laisi
 */
class DatabaseRule extends ExternalResource {

    final ConnectionPoolBuilder builder;
    private static PostgresProcess process;
    ConnectionPool pool;

    DatabaseRule() {
        this(createPoolBuilder(1));
    }

    DatabaseRule(ConnectionPoolBuilder builder) {
        this.builder = builder;
        if (builder instanceof EmbeddedConnectionPoolBuilder) {
            /*
            if (process == null) {
                try {
                    PostgresStarter<PostgresExecutable, PostgresProcess> runtime = PostgresStarter.getDefaultInstance();
                    //PostgresConfig config = new PostgresConfig(V11_1, new AbstractPostgresConfig.Net(),
                    PostgresConfig config = new PostgresConfig(V11_1, new AbstractPostgresConfig.Net("localhost", 54321),
                            new AbstractPostgresConfig.Storage("async-pg"), new AbstractPostgresConfig.Timeout(),
                            new AbstractPostgresConfig.Credentials("async-pg", "async-pg"));
                    PostgresExecutable exec = runtime.prepare(config);
                    process = exec.start();

                    out.printf("Started postgres to %s:%d%n", process.getConfig().net().host(), process.getConfig().net().port());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            builder.hostname(process.getConfig().net().host());
            builder.port(process.getConfig().net().port());
            */

            builder.hostname("localhost");
            builder.port(54321);

        }
    }

    @Override
    protected void before() {
        if (pool == null) {
            pool = builder.build();
        }
    }

    @Override
    protected void after() {
        if (pool != null) {
            try {
                pool.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    ResultSet query(String sql) {
        return block(db().completeQuery(sql));
    }

    ResultSet query(String sql, List<?> params) {
        return block(db().completeQuery(sql, params.toArray()));
    }

    Collection<ResultSet> script(String sql) {
        return block(db().completeScript(sql));
    }

    private <T> T block(CompletableFuture<T> future) {
        try {
            return future.get(5_0000000, TimeUnit.SECONDS);
        } catch (Throwable th) {
            throw new RuntimeException(th);
        }
    }

    Db db() {
        before();
        return pool;
    }

    static class EmbeddedConnectionPoolBuilder extends ConnectionPoolBuilder {
        EmbeddedConnectionPoolBuilder() {
            database("async-pg");
            username("async-pg");
            password("async-pg");
            encoding(System.getProperty("file.encoding", "utf-8"));
        }
    }

    static ConnectionPool createPool(int size) {
        return createPoolBuilder(size).build();
    }

    static ConnectionPoolBuilder createPoolBuilder(int size) {
        String db = getenv("PG_DATABASE");
        String user = getenv("PG_USERNAME");
        String pass = getenv("PG_PASSWORD");

        if (db == null && user == null && pass == null) {
            return new EmbeddedConnectionPoolBuilder().maxConnections(size);
        } else {
            return new EmbeddedConnectionPoolBuilder()
                    .database(envOrDefault("PG_DATABASE", "postgres"))
                    .username(envOrDefault("PG_USERNAME", "postgres"))
                    .password(envOrDefault("PG_PASSWORD", "postgres"))
                    .ssl(true)
                    .maxConnections(size);
        }
    }


    static String envOrDefault(String var, String def) {
        String value = getenv(var);
        return value != null ? value : def;
    }
}
