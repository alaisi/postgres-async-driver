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

import java.util.concurrent.CompletableFuture;

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
    CompletableFuture<Void> commit();

    /**
     * Rollbacks a transaction.
     */
    CompletableFuture<Void> rollback();

    /**
     * Commits a transaction and rollbacks it if an error occurs.
     */
    CompletableFuture<Void> close();

}
