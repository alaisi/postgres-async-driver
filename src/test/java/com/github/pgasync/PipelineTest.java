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

import com.pgasync.Connectible;
import com.pgasync.Connection;
import com.pgasync.SqlException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.SynchronousQueue;
import java.util.stream.IntStream;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

/**
 * Tests for statements pipelining.
 *
 * @author Mikko Tiihonen
 * @author Marat Gainullin
 */
public class PipelineTest {

    @ClassRule
    public static DatabaseRule dbr = new DatabaseRule();

    private Connection c;
    private Connectible pool;

    @Before
    public void setupPool() {
        pool = dbr.builder.pool();
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
        SECONDS.sleep(2 + remoteWaitTimeSeconds);
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

    @Test(expected = SqlException.class)
    public void messageStreamEnsuresSequentialAccess() throws Exception {
        Connection connection = getConnection();
        try {
            CompletableFuture.allOf((CompletableFuture<?>[]) IntStream.range(0, 10).mapToObj(i -> connection.completeQuery("select " + i + ", pg_sleep(" + 10 + ")")
                            .exceptionally(th -> {
                                throw new IllegalStateException(new SqlException(th));
                            })
                    ).toArray(size -> new CompletableFuture<?>[size])
            ).get();
        } catch (Exception ex) {
            SqlException.ifCause(ex, sqlException -> {
                throw sqlException;
            }, () -> {
                throw ex;
            });
        }
    }

}
