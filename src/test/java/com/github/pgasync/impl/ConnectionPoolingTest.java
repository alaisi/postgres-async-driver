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

import com.github.pgasync.ResultSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

/**
 * Tests for connection pool concurrency.
 * 
 * @author Antti Laisi
 */
public class ConnectionPoolingTest {

    @Rule
    public final DatabaseRule dbr = new DatabaseRule();

    @Before
    public void create() {
        dbr.query("DROP TABLE IF EXISTS CP_TEST; CREATE TABLE CP_TEST (ID VARCHAR(255) PRIMARY KEY)");
    }

    @After
    public void drop() {
        dbr.query("DROP TABLE CP_TEST");
    }

    @Test
    public void shouldRunAllQueuedCallbacks() throws Exception {
        final int count = 1000;
        IntFunction<Callable<ResultSet>> insert = value -> () -> dbr.query("INSERT INTO CP_TEST VALUES($1)", singletonList(value));
        List<Callable<ResultSet>> tasks = IntStream.range(0, count).mapToObj(insert).collect(toList());

        ExecutorService executor = Executors.newFixedThreadPool(20);
        executor.invokeAll(tasks).stream().map(this::await);

        assertEquals(count, dbr.query("SELECT COUNT(*) FROM CP_TEST").row(0).getLong(0).longValue());
    }

    <T> T await(Future<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
