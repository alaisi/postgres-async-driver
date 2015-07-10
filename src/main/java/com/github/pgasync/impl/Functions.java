/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
                Function::apply,
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
