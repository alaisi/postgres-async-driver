package com.github.pgasync.impl;

import static java.lang.System.getenv;
import static java.lang.System.out;
import static ru.yandex.qatools.embed.postgresql.distribution.Version.V9_6_2;

import com.github.pgasync.ConnectionPool;
import com.github.pgasync.ConnectionPoolBuilder;
import com.github.pgasync.Db;
import com.github.pgasync.ResultSet;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.util.List;

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
    static PostgresProcess process;
    ConnectionPool pool;

    DatabaseRule() {
        this(createPoolBuilder(1));
    }

    DatabaseRule(ConnectionPoolBuilder builder) {
        this.builder = builder;
        if (builder instanceof EmbeddedConnectionPoolBuilder)
        {
            if (process == null)
            {
                try
                {
                    PostgresStarter<PostgresExecutable, PostgresProcess> runtime = PostgresStarter.getDefaultInstance();
                    PostgresConfig config = new PostgresConfig(V9_6_2, new AbstractPostgresConfig.Net(),
                        new AbstractPostgresConfig.Storage("async-pg"), new AbstractPostgresConfig.Timeout(),
                        new AbstractPostgresConfig.Credentials("async-pg", "async-pg"));
                    PostgresExecutable exec = runtime.prepare(config);
                    process = exec.start();

                    out.printf("Started postgres to %s:%d%n", process.getConfig().net().host(), process.getConfig().net().port());
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }

            builder.hostname(process.getConfig().net().host());
            builder.port(process.getConfig().net().port());
        }
    }

    @Override
    protected void before() {
        if(pool == null) {
            pool = builder.build();
        }
    }

    @Override
    protected void after() {
        if(pool != null) {
            try {
                pool.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    ResultSet query(String sql) {
        return db().querySet(sql, (Object[]) null).blockingGet();
    }
    @SuppressWarnings("rawtypes")
    ResultSet query(String sql, List/*<Object>*/ params) {
        return db().querySet(sql, params.toArray()).blockingGet();
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
            return new EmbeddedConnectionPoolBuilder().poolSize(size);
        }
        return new EmbeddedConnectionPoolBuilder()
                .database(envOrDefault("PG_DATABASE", "postgres"))
                .username(envOrDefault("PG_USERNAME", "postgres"))
                .password(envOrDefault("PG_PASSWORD", "postgres"))
                .ssl(true)
                .poolSize(size);
    }


    static String envOrDefault(String var, String def) {
        String value = getenv(var);
        return value != null ? value : def;
    }
}
