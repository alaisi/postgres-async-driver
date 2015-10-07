package com.github.pgasync.impl.reactive;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class ObservableTest {

    @Test
    public void shouldFlatMap() {
        assertEquals(asList(1, 2, 2, 4, 3, 6),
                Observable.just(1, 2, 3)
                        .flatMap(n -> Observable.just(n, n * 2))
                        .toList()
                        .toBlocking()
                        .single());
    }

    @Test
    public void shouldMap() {
        assertEquals(asList(2, 3, 4),
                Observable.just(1, 2, 3)
                        .map(n -> n + 1)
                        .toList()
                        .toBlocking()
                        .single());
    }

    @Test
    public void shouldLift() {
        assertEquals(asList(2, 4, 6),
                Observable.just(1, 2, 3)
                        .lift(subscriber -> new Subscriber<Integer>() {
                            @Override
                            public void onSubscribe(Subscription s) {
                                subscriber.onSubscribe(s);
                            }
                            @Override
                            public void onNext(Integer integer) {
                                subscriber.onNext(2 * integer);
                            }
                            @Override
                            public void onError(Throwable t) {
                                subscriber.onError(t);
                            }
                            @Override
                            public void onComplete() {
                                subscriber.onComplete();
                            }
                        })
                        .toList()
                        .toBlocking()
                        .single());
    }

    @Test
    public void shouldOnErrorResumeNext() {
        assertEquals(asList(1, 10, 20),
                Observable.create(subscriber -> {
                    subscriber.onNext(1);
                    subscriber.onError(new Exception());
                })
                .onErrorResumeNext(throwable -> Observable.just(10, 20))
                .toList()
                .toBlocking()
                .single());
    }

}
