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

import static java.lang.Long.MIN_VALUE;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.out;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.runners.MethodSorters.NAME_ASCENDING;
import java.util.ArrayList;
import java.util.Collection;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import com.github.pgasync.Connection;
import com.github.pgasync.ConnectionPool;

@RunWith(Parameterized.class)
@FixMethodOrder(NAME_ASCENDING)
public class PerformanceTest {

    @Parameters(name = "{index}: poolSize={0}, threads={1}, pipeline={2}")
    public static Iterable<Object[]> data() {
        results = new TreeMap<>();
        ArrayList<Object[]> testData = new ArrayList<>();
        for (int poolSize = 1; poolSize <= 4; poolSize *= 2) {
            results.putIfAbsent(key(poolSize, true), new TreeMap<>());
            results.putIfAbsent(key(poolSize, false), new TreeMap<>());
            for (int threads = 1; threads <= 16; threads *= 2) {
                testData.add(new Object[] { poolSize, threads, true });
                testData.add(new Object[] { poolSize, threads, false });
            }
        }
        return testData;
    }

    private static String key(int poolSize, boolean pipeline) {
        return poolSize + " conn" + (pipeline ? "/pipeline" : "");
    }

    private static final int batchSize = 100;
    private static final int repeats = 5;
    private static final Consumer<Throwable> err = t -> {t.printStackTrace();
        throw new AssertionError(t);
    };
    private static SortedMap<String, SortedMap<Integer, Long>> results = new TreeMap<>();
    private final int poolSize;
    private final int numThreads;
    private final boolean pipeline;
    private final ConnectionPool dbPool;
    private final ExecutorService threadPool;

    public PerformanceTest(int poolSize, int numThreads, boolean pipeline) {
        this.poolSize = poolSize;
        this.numThreads = numThreads;
        this.pipeline = pipeline;
        dbPool = DatabaseRule.createPoolBuilder(poolSize).pipeline(pipeline).validationQuery(null).build();
        threadPool = Executors.newFixedThreadPool(numThreads);
    }

    @After
    public void close() throws Exception {
        threadPool.shutdownNow();
        dbPool.close();
    }

    @Test(timeout = 1000)
    public void t1_preAllocatePool() throws InterruptedException {
        Queue<Connection> connections = new ArrayBlockingQueue<>(poolSize);
        for (int i = 0; i < poolSize; ++i) {
            dbPool.getConnection().subscribe(connections::add);
        }
        while (connections.size() < poolSize) {
            MILLISECONDS.sleep(5);
        }
        connections.forEach(dbPool::release);
    }

    @Test
    public void t3_run() throws Exception {
        Collection<Callable<Long>> tasks = new ArrayList<>();
        for (int i = 0; i < batchSize; ++i) {
            tasks.add(new Callable<Long>() {
                final Exchanger<Long> swap = new Exchanger<>();

                @Override
                public Long call() throws Exception {
                    dbPool.query("select 42", r -> {
                        try {
                            swap.exchange(currentTimeMillis());
                        } catch (Exception e) {
                            err.accept(e);
                        }
                    }, err);
                    return swap.exchange(null);
                }
            });
        }

        long minTime = Long.MAX_VALUE;

        for (int r = 0; r < repeats; ++r) {
            System.gc();
            MILLISECONDS.sleep(300);

            final CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);

            final Queue<Callable<Long>> taskQueue = new LinkedBlockingQueue<>(tasks);
            final Queue<Long> endTimes = new ArrayBlockingQueue<>(batchSize);

            Thread[] threads = new Thread[numThreads];
            for (int i = 0; i < numThreads; ++i) {
                threads[i] = new Thread("tester" + i) {
                    public void run() {
                        try {
                            barrier.await();
                        } catch (InterruptedException | BrokenBarrierException e) {
                            e.printStackTrace();
                        }

                        Callable<Long> c;
                        try {
                            while ((c = taskQueue.poll()) != null) {
                                endTimes.add(c.call());
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                };
                threads[i].start();
            }

            long start = currentTimeMillis();
            barrier.await();

            for (Thread thread : threads) {
                thread.join();
            }

            OptionalLong end = endTimes.stream().mapToLong(f -> f).max();
            long time = end.getAsLong() - start;
            minTime = Math.min(minTime, time);
        }

        results.get(key(poolSize, pipeline)).put(numThreads, minTime);

        out.printf("%d%s,%2d,%4.3f%n", poolSize, pipeline ? "p" : "n", numThreads, minTime / 1000.0);
    }

    @AfterClass
    public static void printCsv() {
        out.print("threads");
        results.keySet().forEach(i -> out.printf(",%s", i));
        out.println();

        results.values().iterator().next().keySet().forEach(threads -> {
            out.print(threads);
            results.keySet().forEach(conns -> {
                long millis = results.get(conns).get(threads);
                double rps = batchSize * 1000 / (double) millis;
                out.printf(",%f", rps);
            });
            out.println();
        });
    }
}