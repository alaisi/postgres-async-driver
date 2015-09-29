package com.github.pgasync.impl.reactive;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.function.Function;

public class AsyncPublisher<T> implements Publisher<T> {

    private final Publisher<T> publisher;

    private AsyncPublisher(Publisher<T> publisher) {
        this.publisher = publisher;
    }

    public static <T> AsyncPublisher<T> just(T item) {
        return new AsyncPublisher<>(subscriber -> {
            subscriber.onNext(item);
            subscriber.onComplete();
        });
    }

    public static <T> AsyncPublisher<T> empty() {
        return new AsyncPublisher<>(Subscriber::onComplete);
    }

    public static <T> AsyncPublisher<T> create(Publisher<T> publisher) {
        return new AsyncPublisher<>(publisher);
    }

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        publisher.subscribe(subscriber);
    }

    public <X> AsyncPublisher<X> flatMap(Function<T,Publisher<X>> f) {
        return new AsyncPublisher<>(subscriber -> publisher.subscribe(new Forward<T,X>(subscriber) {
            @Override
            public void onNext(T t) {
                f.apply(t).subscribe(new Forward<>(subscriber));
            }
        }));
    }

    public static class Forward<F,R> implements Subscriber<F> {

        protected final Subscriber<? super R> to;

        public Forward(Subscriber<? super R> to) {
            this.to = to;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            to.onSubscribe(subscription);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onNext(F t) {
            to.onNext((R) t);
        }

        @Override
        public void onError(Throwable throwable) {
            to.onError(throwable);
        }

        @Override
        public void onComplete() {
            to.onComplete();
        }
    }
}
