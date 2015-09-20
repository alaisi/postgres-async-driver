package com.github.pgasync.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Collections.unmodifiableMap;

public class EventEmitter<T> {

    private final Consumer<Emitter<T>> onListen;

    private EventEmitter(Consumer<Emitter<T>> onListen) {
        this.onListen = onListen;
    }

    public static <X> EventEmitter<X> create(Consumer<Emitter<X>> onListen) {
        return new EventEmitter<>(onListen);
    }

    public <X extends T, E extends Throwable> void on(Class<X> match1, Consumer<X> consumer1,
                                                      Class<E> error, Consumer<E> onError) {
        Map<Class<?>,Consumer<?>> matches = new HashMap<>();
        matches.put(match1, consumer1);
        matches.put(error, onError);
        onListen.accept(new Emitter<>(unmodifiableMap(matches)));
    }

    public <X extends T, Y extends T, E extends Throwable> void on(Class<X> match1, Consumer<X> consumer1,
                                                                   Class<Y> match2, Consumer<Y> consumer2,
                                                                   Class<E> error, Consumer<E> onError) {
        Map<Class<?>,Consumer<?>> matches = new HashMap<>();
        matches.put(match1, consumer1);
        matches.put(match2, consumer2);
        matches.put(error, onError);
        onListen.accept(new Emitter<>(unmodifiableMap(matches)));
    }

    public static class Emitter<T> {

        private final Map<Class<?>,Consumer<?>> consumers;

        private Emitter(Map<Class<?>, Consumer<?>> consumers) {
            this.consumers = consumers;
        }

        @SuppressWarnings("unchecked")
        public <X extends T> void emit(Class<X> type, X event) {
            Consumer<X> consumer = (Consumer<X>) consumers.get(type);
            consumer.accept(event);
        }

        @SuppressWarnings("unchecked")
        public void error(Throwable e) {
            Consumer<Throwable> consumer = (Consumer<Throwable>) consumers.get(Throwable.class);
            consumer.accept(e);
        }
    }

}
