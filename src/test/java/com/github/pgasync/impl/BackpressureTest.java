package com.github.pgasync.impl;

import org.junit.ClassRule;
import org.junit.Test;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.pgasync.impl.DatabaseRule.createPoolBuilder;
import static org.junit.Assert.assertEquals;

public class BackpressureTest {
    @ClassRule
    public static DatabaseRule dbr = new DatabaseRule(createPoolBuilder(1));

    @Test
    public void shouldRespectBackpressureOnSlowConsumer() {
        //given
        int numberOfRecords = 1000;

        dbr.query("CREATE TABLE BACKPRESSURE_TEST(ID VARCHAR)");
        Observable.merge(
                IntStream
                        .range(0, numberOfRecords)
                        .mapToObj(__ -> dbr.db().querySet("INSERT INTO BACKPRESSURE_TEST VALUES($1)", UUID.randomUUID().toString()))
                        .collect(Collectors.toList())
        ).toCompletable().await(10, TimeUnit.SECONDS);

        AtomicInteger counter = new AtomicInteger();

        // when
        dbr.db().queryRows("SELECT * FROM BACKPRESSURE_TEST")
                .observeOn(Schedulers.computation())
                .doOnNext(__ -> {
                    sleep();
                    counter.incrementAndGet();
                })
                .toBlocking()
                .subscribe();

        // then
        assertEquals(numberOfRecords, counter.get());
    }

    private void sleep() {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
