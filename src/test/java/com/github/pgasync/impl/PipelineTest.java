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

import com.github.pgasync.Connection;
import com.github.pgasync.ConnectionPool;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Deque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

/**
 * Tests for statement pipelining.
 *
 * @author Mikko Tiihonen
 */
public class PipelineTest {

    @ClassRule
    public static DatabaseRule dbr = new DatabaseRule();

    private Connection c;
    private ConnectionPool pool;

    @Before
    public void setupPool() {
        pool = dbr.builder.build();
    }

    @After
    public void closePool() throws Exception {
        if (c != null) {
            c.close().get();
        }
        if (pool != null) {
            pool.close().get();
        }
    }

    @Test
    public void connectionPoolPipelinesQueries() throws InterruptedException {
        int count = 5;
        double sleep = 0.5;
        Deque<Long> results = new LinkedBlockingDeque<>();
        long startWrite = currentTimeMillis();
        for (int i = 0; i < count; ++i) {
            pool.completeQuery("select " + i + ", pg_sleep(" + sleep + ")")
                    .thenAccept(r -> results.add(currentTimeMillis()))
                    .exceptionally(th -> {
                        throw new AssertionError("failed", th);
                    });
        }
        long writeTime = currentTimeMillis() - startWrite;

        long remoteWaitTimeSeconds = (long) (sleep * count);
        SECONDS.sleep(1 + remoteWaitTimeSeconds);
        long readTime = results.getLast() - results.getFirst();

        assertThat(results.size(), is(count));
        assertThat(MILLISECONDS.toSeconds(writeTime), is(0L));
        assertThat(MILLISECONDS.toSeconds(readTime + 999) >= remoteWaitTimeSeconds, is(true));
    }

    private Connection getConnection() throws InterruptedException {
        SynchronousQueue<Connection> connQueue = new SynchronousQueue<>();
        pool.getConnection()
                .thenAccept(connQueue::offer);
        return c = connQueue.take();
    }

    @Test
    public void connectionPipelinesQueries() throws InterruptedException {
        Connection c = getConnection();

        int count = 5;
        double sleep = 0.5;
        Deque<Long> results = new LinkedBlockingDeque<>();
        long startWrite = currentTimeMillis();
        for (int i = 0; i < count; ++i) {
            c.completeQuery("select " + i + ", pg_sleep(" + sleep + ")")
                    .thenAccept(r -> results.add(currentTimeMillis()))
                    .exceptionally(th -> {
                        throw new AssertionError("failed", th);
                    });
        }
        long writeTime = currentTimeMillis() - startWrite;

        long remoteWaitTimeSeconds = (long) (sleep * count);
        SECONDS.sleep(1 + remoteWaitTimeSeconds);
        long readTime = results.getLast() - results.getFirst();

        assertThat(results.size(), is(count));
        assertThat(MILLISECONDS.toSeconds(writeTime), is(0L));
        assertThat(MILLISECONDS.toSeconds(readTime + 999) >= remoteWaitTimeSeconds, is(true));
    }

    @Test
    public void connectionPoolPipelinesQueriesWithinTransaction() throws InterruptedException {
        int count = 5;
        double sleep = 0.5;
        Deque<Long> results = new LinkedBlockingDeque<>();
        AtomicLong writeTime = new AtomicLong();

        CountDownLatch sync = new CountDownLatch(1);
        long startWrite = currentTimeMillis();
        pool.begin()
                .thenAccept(transaction -> {
                    for (int i = 0; i < count; ++i) {
                        transaction.completeQuery("select " + i + ", pg_sleep(" + sleep + ")")
                                .thenAccept(r -> results.add(currentTimeMillis()))
                                .exceptionally(th -> {
                                    throw new AssertionError("failed", th);
                                });
                    }
                    transaction.commit()
                            .thenAccept(v -> sync.countDown())
                            .exceptionally(th -> {
                                throw new AssertionError("failed", th);
                            });
                    writeTime.set(currentTimeMillis() - startWrite);
                })
                .exceptionally(th -> {
                    throw new AssertionError("failed", th);
                });
        sync.await(3, SECONDS);

        long remoteWaitTimeSeconds = (long) (sleep * count);
        SECONDS.sleep(1 + remoteWaitTimeSeconds);
        long readTime = results.getLast() - results.getFirst();

        assertThat(results.size(), is(count));
        assertThat(MILLISECONDS.toSeconds(writeTime.get()), is(0L));
        assertThat(MILLISECONDS.toSeconds(readTime + 999) >= remoteWaitTimeSeconds, is(true));
    }

}
