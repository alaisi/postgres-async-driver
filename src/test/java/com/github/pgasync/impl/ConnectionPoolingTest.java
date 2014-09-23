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

import com.github.pgasync.ConnectionPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for connection pool concurrency.
 * 
 * @author Antti Laisi
 */
public class ConnectionPoolingTest {

    final Consumer<Throwable> err = t -> { throw new AssertionError("failed", t); };
    final ConnectionPool pool = DatabaseRule.createPool(10);

    @Before
    public void create() {
        ResultHolder result = new ResultHolder();
        pool.query("DROP TABLE IF EXISTS CP_TEST; CREATE TABLE CP_TEST (ID VARCHAR(255) PRIMARY KEY)",
                result, result.errorHandler());
        result.result();
    }

    @After
    public void drop() {
        ResultHolder result = new ResultHolder();
        pool.query("DROP TABLE CP_TEST", result, result.errorHandler());
        result.result();
        pool.close();
    }

    @Test
    public void shouldRunAllQueuedCallbacks() throws Exception {
        
        final AtomicInteger count = new AtomicInteger(); 
        final CountDownLatch latch = new CountDownLatch(1000);

        for(int i = 0; i < 20; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    final Queue<Runnable> queries = new LinkedList<>();
                    for(int j = 0; j < 50; j++) {
                        queries.add(() -> pool.query("INSERT INTO CP_TEST VALUES($1)", asList(UUID.randomUUID()), result -> {
                            latch.countDown();
                            count.incrementAndGet();
                            if(!queries.isEmpty()) {
                                queries.poll().run();
                            }
                        }, err));
                    }
                    queries.poll().run();
                }
            }).start();
        }
        assertTrue(latch.await(5L, TimeUnit.SECONDS));
        assertEquals(1000, count.get());

        ResultHolder result = new ResultHolder();
        pool.query("SELECT COUNT(*) FROM CP_TEST", result, result.errorHandler());
        assertEquals(count.get(), result.result().row(0).getLong(0).longValue());
    }
}
