package com.github.pgasync.impl;

import com.github.pgasync.ConnectionPool;
import com.github.pgasync.ConnectionPoolBuilder;
import com.github.pgasync.Db;
import com.github.pgasync.ResultSet;
import org.junit.rules.ExternalResource;
import rx.Observable;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Antti Laisi
 */
class DatabaseRule extends ExternalResource {

    final ConnectionPoolBuilder builder;
    ConnectionPool pool;

    DatabaseRule() {
        this(createPoolBuilder(1));
    }

    DatabaseRule(ConnectionPoolBuilder builder) {
        this.builder = builder;
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
        return block(db().querySet(sql, (Object[]) null));
    }
    @SuppressWarnings("rawtypes")
    ResultSet query(String sql, List/*<Object>*/ params) {
        return block(db().querySet(sql, params.toArray()));
    }

    private <T> T block(Observable<T> observable) {
        BlockingQueue<Entry<T,Throwable>> result = new ArrayBlockingQueue<>(1);
        observable.single().subscribe(
                item -> result.add(new SimpleImmutableEntry<>(item, null)),
                exception -> result.add(new SimpleImmutableEntry<>(null, exception)));
        try {

            Entry<T,Throwable> entry = result.poll(5, TimeUnit.SECONDS);
            if(entry == null) {
                throw new RuntimeException("Timed out waiting for result");
            }
            Throwable exception = entry.getValue();
            if(exception != null) {
                throw exception instanceof RuntimeException
                        ? (RuntimeException) exception
                        : new RuntimeException(exception);
            }
            return entry.getKey();

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    Db db() {
        before();
        return pool;
    }

    static ConnectionPool createPool(int size) {
        return createPoolBuilder(size).build();
    }
    static ConnectionPoolBuilder createPoolBuilder(int size) {
        return new ConnectionPoolBuilder()
                .database(envOrDefault("PG_DATABASE", "postgres"))
                .username(envOrDefault("PG_USERNAME", "postgres"))
                .password(envOrDefault("PG_PASSWORD", "postgres"))
                .ssl(true)
                .poolSize(size);
    }


    static String envOrDefault(String var, String def) {
        String value = System.getenv(var);
        return value != null ? value : def;
    }
}
