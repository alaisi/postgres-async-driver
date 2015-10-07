package com.github.pgasync.impl.reactive;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import rx.functions.Func1;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

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

    public static <T> Observable<T> just(T item, T... items) {
        return new Observable<>(subscriber -> {
            subscriber.onNext(item);
            for(T t : items) {
                subscriber.onNext(t);
            }
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
        publisher.subscribe(new Sub<T>() {
            @Override
            public void doOnSubscribe(Subscription subscription) {
                subscription.request(1);
            }

            @Override
            public void doOnNext(T t) {
                if (onNext != null) {
                    onNext.accept(t);
                }
            }

            @Override
            public void doOnError(Throwable throwable) {
                if (onError != null) {
                    onError.accept(throwable);
                }
            }

            @Override
            public void doOnComplete() {
                if (onComplete != null) {
                    onComplete.run();
                }
            }
        });
    }

    public <X> Observable<X> map(Function<T,X> f) {
        return new Observable<>(subscriber -> publisher.subscribe(new Forward<T,X>(subscriber) {
            @Override
            public void doOnNext(T t) {
                subscriber.onNext(f.apply(t));
            }
        }));
    }

    public <X> Observable<X> flatMap(Function<T,Observable<X>> f) {
        return new Observable<>(subscriber -> publisher.subscribe(new Forward<T,X>(subscriber) {
            @Override
            public void doOnNext(T t) {
                f.apply(t).subscribe(new Forward<>(subscriber));
            }
        }));
    }

    public <X> Observable<X> lift(Operator<X,T> operator) {
        return new Observable<>(subscriber -> publisher.subscribe(operator.apply(subscriber)));
    }

    public Observable<T> single(Function<T,Boolean> f) {
        return new Observable<>(subscriber -> publisher.subscribe(new Forward<T,T>(subscriber) {
            @Override
            public void doOnNext(T t) {
                if(f.apply(t)) {
                    super.doOnNext(t);
                }
            }
        }));
    }

    public Observable<T> onErrorResumeNext(Function<Throwable, Observable<T>> f) {
        return new Observable<>(subscriber -> publisher.subscribe(new Forward<T,T>(subscriber) {
            @Override
            public void doOnError(Throwable throwable) {
                f.apply(throwable).subscribe(new Forward<>(subscriber));
            }
        }));
    }

    public Observable<T> doOnTerminate(Runnable f) {
        return new Observable<>(subscriber -> publisher.subscribe(new Forward<T,T>(subscriber) {
            @Override
            public void doOnError(Throwable throwable) {
                f.run();
                subscriber.onError(throwable);
            }
            @Override
            public void doOnComplete() {
                f.run();
                subscriber.onComplete();
            }
        }));
    }

    public Observable<T> doOnNext(Consumer<T> f) {
        return new Observable<>(subscriber -> publisher.subscribe(new Forward<T,T>(subscriber) {
            @Override
            public void doOnNext(T t) {
                f.accept(t);
                super.doOnNext(t);
            }
        }));
    }

    public Observable<T> doOnError(Consumer<Throwable> f) {
        return new Observable<>(subscriber -> publisher.subscribe(new Forward<T,T>(subscriber) {
            @Override
            public void doOnError(Throwable throwable) {
                f.accept(throwable);
                subscriber.onError(throwable);
            }
        }));
    }

    public Observable<T> doOnSubscribe(Runnable f) {
        return new Observable<>(subscriber -> publisher.subscribe(new Forward<T,T>(subscriber) {
            @Override
            public void doOnSubscribe(Subscription subscription) {
                f.run();
                subscriber.onSubscribe(subscription);
            }
        }));
    }

    public Observable<T> doOnUnsubscribe(Runnable f) {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Observable<List<T>> toList() {
        return new Observable<>(subscriber -> {

            List<T> list = new LinkedList<>();

            publisher.subscribe(new Forward<T,List<T>>(subscriber) {
                @Override
                public void doOnNext(T t) {
                    list.add(t);
                }
                @Override
                public void doOnComplete() {
                    subscriber.onNext(list);
                    subscriber.onComplete();
                }
            });
        });
    }

    public BlockingObservable<T> toBlocking() {
        return new BlockingObservable<>(publisher);
    }

    public interface Operator<R, T> extends Function<Subscriber<? super R>, Subscriber<? super T>> {

    }

    public abstract static class Sub<T> implements Subscriber<T> {

        @Override
        public final void onSubscribe(Subscription s) {
            try {
                doOnSubscribe(s);
            } catch (Throwable t) {
                onError(t);
            }
        }
        @Override
        public final void onNext(T t) {
            try {
                doOnNext(t);
            } catch (Throwable throwable) {
                onError(throwable);
            }
        }
        @Override
        public final void onError(Throwable throwable) {
            try {
                doOnError(throwable);
            } catch (Throwable err) {
                Logger.getLogger(getClass().getName()).log(Level.SEVERE, "onError threw exception", err);
            }
        }
        @Override
        public final void onComplete() {
            try {
                doOnComplete();
            } catch (Throwable throwable) {
                onError(throwable);
            }
        }
        protected abstract void doOnSubscribe(Subscription s);
        protected abstract void doOnNext(T t);
        protected abstract void doOnError(Throwable throwable);
        protected abstract void doOnComplete();
    }

    public static class Forward<F,R> extends Sub<F> {

        protected final Subscriber<? super R> to;

        public Forward(Subscriber<? super R> to) {
            this.to = to;
        }

        @Override
        public void doOnSubscribe(Subscription subscription) {
            to.onSubscribe(subscription);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void doOnNext(F t) {
            to.onNext((R) t);
        }

        @Override
        public void doOnError(Throwable throwable) {
            to.onError(throwable);
        }

        @Override
        public void doOnComplete() {
            to.onComplete();
        }
    }

    public static class BlockingObservable<T> {

        private final Publisher<T> source;
        private final BlockingQueue<Entry<T,Throwable>> result = new LinkedBlockingQueue<>();

        public BlockingObservable(Publisher<T> source) {
            this.source = source;
        }

        public T single() {

            source.subscribe(new Subscriber<T>() {
                @Override
                public void onNext(T t) {
                    result.add(new SimpleImmutableEntry<>(t, null));
                }
                @Override
                public void onError(Throwable t) {
                    result.add(new SimpleImmutableEntry<>(null, t));
                }
                @Override
                public void onSubscribe(Subscription s) { }
                @Override
                public void onComplete() { }
            });

            try {
                Entry<T, Throwable> entry = result.take();
                if(entry.getValue() != null) {
                    throw entry.getValue() instanceof RuntimeException
                            ? (RuntimeException) entry.getValue()
                            : new RuntimeException(entry.getValue());
                }
                return entry.getKey();

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
