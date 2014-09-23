package com.github.pgasync.impl;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Utility methods for functions.
 *
 * @author Antti Laisi
 */
enum Functions {
    ;

    static final Logger LOG = Logger.getLogger(Functions.class.getName());

    static <A extends Function<T,A> & Supplier<R>, T, R> Supplier<R> reduce(A accumulator, Stream<T> stream) {
        return stream.reduce(
                accumulator,
                (acc, t) -> acc.apply(t),
                (l, r) -> { throw new UnsupportedOperationException(); });
    }

    static <T> void applyConsumer(Consumer<T> consumer, T value, Consumer<Throwable> onError) {
        try {
            consumer.accept(value);
        } catch (Throwable t) {
            applyConsumer(onError, t);
        }
    }

    static <T> void applyConsumer(Consumer<T> consumer, T value) {
        if (consumer != null) {
            try {
                consumer.accept(value);
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "Consumer " + consumer + " failed with exception", e);
            }
        } else {
            LOG.log(Level.SEVERE, "No consumer to handle value: " + value);
        }
    }

    static void applyRunnable(Runnable runnable, Consumer<Throwable> onError) {
        try {
            runnable.run();
        } catch (Throwable t) {
            applyConsumer(onError, t);
        }
    }

}
