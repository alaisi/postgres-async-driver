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

import java.util.function.Consumer;

/**
 * Tests for statement pipelining.
 *
 * @author Mikko Tiihonen
 */
public class PipelineTest {
    final Consumer<Throwable> err = t -> { throw new AssertionError("failed", t); };

    Connection c;
    ConnectionPool pool;
/*
    @After
    public void closeConnection() {
        if (c != null) {
            pool.release(c);
        }
        if (pool != null) {
            pool.close();
        }
    }

    @Test
    public void connectionPipelinesQueries() throws InterruptedException {
        pool = createPoolBuilder(1).pipeline(true).build();

        int count = 5;
        double sleep = 0.5;
        Deque<Long> results = new LinkedBlockingDeque<>();
        long startWrite = currentTimeMillis();
        for (int i = 0; i < count; ++i) {
            pool.queryRows("select " + i + ", pg_sleep(" + sleep + ")", r -> results.add(currentTimeMillis()),
                    err);
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
        pool = createPoolBuilder(1).pipeline(pipeline).build();
        SynchronousQueue<Connection> connQueue = new SynchronousQueue<>();
        pool.getConnection(c -> connQueue.add(c), err);
        return c = connQueue.take();
    }

    @Test
    public void disabledConnectionPipeliningThrowsErrorWhenPipeliningIsAttempted() throws Exception {
        Connection c = getConnection(false);

        BlockingQueue<ResultSet> rs = new LinkedBlockingDeque<>();
        BlockingQueue<Throwable> err = new LinkedBlockingDeque<>();
        for (int i = 0; i < 2; ++i) {
            c.queryRows("select " + i + ", pg_sleep(0.5)", r -> rs.add(r), e -> err.add(e));
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
            c.queryRows("select " + i + ", pg_sleep(" + sleep + ")", r -> results.add(currentTimeMillis()),
                    err);
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
        pool = createPoolBuilder(1).pipeline(true).build();

        int count = 5;
        double sleep = 0.5;
        Deque<Long> results = new LinkedBlockingDeque<>();
        AtomicLong writeTime = new AtomicLong();

        CountDownLatch sync = new CountDownLatch(1);
        long startWrite = currentTimeMillis();
        pool.begin(t -> {
            for (int i = 0; i < count; ++i) {
                t.queryRows("select " + i + ", pg_sleep(" + sleep + ")", r -> results.add(currentTimeMillis()),
                        err);
            }
            t.commit(() -> {
                sync.countDown();
            } , err);
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
    */
}
