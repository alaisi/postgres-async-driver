package com.github.pgasync.impl.reactive;

import org.junit.Test;

public class ObservableTest {

    @Test
    public void shouldFlatMap() {
        Observable.create(subscriber -> {
            subscriber.onNext("foo");
            subscriber.onNext("bar");
            subscriber.onComplete();
        }).subscribe(System.out::println);
    }

}
