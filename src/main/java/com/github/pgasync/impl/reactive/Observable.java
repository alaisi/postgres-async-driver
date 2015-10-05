package com.github.pgasync.impl.reactive;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.function.Consumer;
import java.util.function.Function;

public class Observable<T> implements Publisher<T> {

    private final Publisher<T> publisher;

    private Observable(Publisher<T> publisher) {
        this.publisher = publisher;
    }

    public static <T> Observable<T> just(T item) {
        return new Observable<>(subscriber -> {
            subscriber.onNext(item);
            subscriber.onComplete();
        });
    }

    public static <T> Observable<T> empty() {
        return new Observable<>(Subscriber::onComplete);
    }

    public static <T> Observable<T> create(Publisher<T> publisher) {
        return new Observable<>(publisher);
    }

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        publisher.subscribe(subscriber);
    }

    public void subscribe(Consumer<? super T> onNext) {
        subscribe(onNext, null, null);
    }

    public void subscribe(Consumer<? super T> onNext, Consumer<? super Throwable> onError) {
        subscribe(onNext, onError, null);
    }

    public void subscribe(Consumer<? super T> onNext, Consumer<? super Throwable> onError, Runnable onComplete) {
        publisher.subscribe(new Subscriber<T>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.request(1);
            }
            @Override
            public void onNext(T t) {
                if(onNext != null) {
                    onNext.accept(t);
                }
            }
            @Override
            public void onError(Throwable throwable) {
                if(onError != null) {
                    onError.accept(throwable);
                }
            }
            @Override
            public void onComplete() {
                if(onComplete != null) {
                    onComplete.run();
                }
            }
        });
    }

    public <X> Observable<X> flatMap(Function<T,Observable<X>> f) {
        return new Observable<>(subscriber -> publisher.subscribe(new Forward<T,X>(subscriber) {
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

    public static class BlockingObservable<T> {

        public T single() {
            return null;
        }

    }
}
