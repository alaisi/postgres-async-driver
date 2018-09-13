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
import com.github.pgasync.ResultSet;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Deque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.github.pgasync.impl.DatabaseRule.createPoolBuilder;
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

    final Consumer<Throwable> err = t -> { throw new AssertionError("failed", t); };

    Connection c;
    ConnectionPool pool;

    @After
    public void closeConnection() throws Exception {
        if (c != null) {
            pool.release(c);
        }
        if (pool != null) {
            pool.close();
        }
    }

    @Test
    public void connectionPipelinesQueries() throws InterruptedException {
        pool = dbr.builder.pipeline(true).build();

        int count = 5;
        double sleep = 0.5;
        Deque<Long> results = new LinkedBlockingDeque<>();
        long startWrite = currentTimeMillis();
        for (int i = 0; i < count; ++i) {
            pool.query("select " + i + ", pg_sleep(" + sleep + ")", r -> results.add(currentTimeMillis()), err);
        }
        long writeTime = currentTimeMillis() - startWrite;

        long remoteWaitTimeSeconds = (long) (sleep * count);
        SECONDS.sleep(1 + remoteWaitTimeSeconds);
        long readTime = results.getLast() - results.getFirst();

        assertThat(results.size(), is(count));
        assertThat(MILLISECONDS.toSeconds(writeTime), is(0L));
        assertThat(MILLISECONDS.toSeconds(readTime + 999) >= remoteWaitTimeSeconds, is(true));
    }

    private Connection getConnection(boolean pipeline) throws InterruptedException {
        pool = dbr.builder.pipeline(pipeline).build();
        SynchronousQueue<Connection> connQueue = new SynchronousQueue<>();
        pool.getConnection().subscribe(connQueue::add);
        return c = connQueue.take();
    }

    @Test @Ignore("TODO: Setup pipeline queue limits")
    public void disabledConnectionPipeliningThrowsErrorWhenPipeliningIsAttempted() throws Exception {
        Connection c = getConnection(false);

        BlockingQueue<ResultSet> rs = new LinkedBlockingDeque<>();
        BlockingQueue<Throwable> err = new LinkedBlockingDeque<>();
        for (int i = 0; i < 2; ++i) {
            c.query("select " + i + ", pg_sleep(0.5)", rs::add, err::add);
        }
        assertThat(err.take().getMessage(), containsString("Pipelining not enabled"));
        assertThat(rs.take(), isA(ResultSet.class));
    }

    @Test
    public void connectionPoolPipelinesQueries() throws InterruptedException {
        Connection c = getConnection(true);

        int count = 5;
        double sleep = 0.5;
        Deque<Long> results = new LinkedBlockingDeque<>();
        long startWrite = currentTimeMillis();
        for (int i = 0; i < count; ++i) {
            c.query("select " + i + ", pg_sleep(" + sleep + ")", r -> results.add(currentTimeMillis()), err);
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
        pool = dbr.builder.pipeline(true).build();

        int count = 5;
        double sleep = 0.5;
        Deque<Long> results = new LinkedBlockingDeque<>();
        AtomicLong writeTime = new AtomicLong();

        CountDownLatch sync = new CountDownLatch(1);
        long startWrite = currentTimeMillis();
        pool.begin(t -> {
            for (int i = 0; i < count; ++i) {
                t.query("select " + i + ", pg_sleep(" + sleep + ")", r -> results.add(currentTimeMillis()), err);
            }
            t.commit(sync::countDown, err);
            writeTime.set(currentTimeMillis() - startWrite);
        } , err);
        sync.await(3, SECONDS);

        long remoteWaitTimeSeconds = (long) (sleep * count);
        SECONDS.sleep(1 + remoteWaitTimeSeconds);
        long readTime = results.getLast() - results.getFirst();

        assertThat(results.size(), is(count));
        assertThat(MILLISECONDS.toSeconds(writeTime.get()), is(0L));
        assertThat(MILLISECONDS.toSeconds(readTime + 999) >= remoteWaitTimeSeconds, is(true));
    }

}
