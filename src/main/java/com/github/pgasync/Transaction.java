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

package com.github.pgasync;

import rx.Observable;

import java.util.function.Consumer;

/**
 * A unit of work. Transactions must be committed or rolled back, otherwise a
 * connection left is a stale state. A rollback is automatically performed after
 * backend error.
 * 
 * @author Antti Laisi
 */
public interface Transaction extends QueryExecutor {

    /**
     * Commits a transaction
     */
    Observable<Void> commit();

    /**
     * Rollbacks a transaction.
     */
    Observable<Void> rollback();

    /**
     * Commits a transaction.
     *
     * @param onCompleted Called when commit completes
     * @param onError Called on exception thrown
     */
    default void commit(Runnable onCompleted, Consumer<Throwable> onError) {
        commit().subscribe(__ -> onCompleted.run(), onError::accept);
    }

    /**
     * Rollbacks a transaction.
     *
     * @param onCompleted Called when rollback completes
     * @param onError Called on exception thrown
     */
    default void rollback(Runnable onCompleted, Consumer<Throwable> onError) {
        rollback().subscribe(__ -> onCompleted.run(), onError::accept);
    }

}
