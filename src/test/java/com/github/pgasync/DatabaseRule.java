package com.github.pgasync;

import static java.lang.System.getenv;

import com.pgasync.ConnectibleBuilder;
import com.pgasync.Connectible;
import com.pgasync.ResultSet;
import de.flapdoodle.embed.process.runtime.Network;
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
import ru.yandex.qatools.embed.postgresql.distribution.Version;

/**
 * @author Antti Laisi
 */
class DatabaseRule extends ExternalResource {

    private static PostgresProcess process;

    final ConnectibleBuilder builder;
    Connectible pool;

    DatabaseRule() {
        this(createPoolBuilder(1));
    }

    DatabaseRule(ConnectibleBuilder builder) {
        this.builder = builder;
        if (builder instanceof EmbeddedConnectionPoolBuilder) {
            String port = System.getProperty("asyncpg.test.postgres.port");
            if (port != null && !port.isBlank()) {
                builder.hostname("localhost");
                builder.port(Integer.valueOf(port));
            } else {
                if (process == null) {
                    try {
                        PostgresStarter<PostgresExecutable, PostgresProcess> runtime = PostgresStarter.getDefaultInstance();
                        PostgresConfig config = new PostgresConfig(Version.V11_1, new AbstractPostgresConfig.Net(),
                                new AbstractPostgresConfig.Storage("async-pg"), new AbstractPostgresConfig.Timeout(),
                                new AbstractPostgresConfig.Credentials("async-pg", "async-pg"));
                        PostgresExecutable exec = runtime.prepare(config);
                        process = exec.start();

                        System.out.printf("Started postgres to %s:%d%n", process.getConfig().net().host(), process.getConfig().net().port());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                builder.hostname(process.getConfig().net().host());
                builder.port(process.getConfig().net().port());
            }
        }
    }

    @Override
    protected void before() {
        if (pool == null) {
            pool = builder.pool();
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
        return block(pool().completeQuery(sql));
    }

    ResultSet query(String sql, List<?> params) {
        return block(pool().completeQuery(sql, params.toArray()));
    }

    Collection<ResultSet> script(String sql) {
        return block(pool().completeScript(sql));
    }

    private <T> T block(CompletableFuture<T> future) {
        try {
            return future.get(5_0000000, TimeUnit.SECONDS);
        } catch (Throwable th) {
            throw new RuntimeException(th);
        }
    }

    Connectible pool() {
        before();
        return pool;
    }

    Connectible plain() {
        before();
        return pool;
    }

    static class EmbeddedConnectionPoolBuilder extends ConnectibleBuilder {
        EmbeddedConnectionPoolBuilder() {
            database("async-pg");
            username("async-pg");
            password("async-pg");
            encoding(System.getProperty("file.encoding", "utf-8"));
        }
    }

    static Connectible createPool(int size) {
        return createPoolBuilder(size).pool();
    }

    static ConnectibleBuilder createPoolBuilder(int size) {
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
