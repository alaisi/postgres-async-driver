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

package com.pgasync.impl;

import com.pgasync.Connection;
import com.pgasync.ConnectionPool;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static java.lang.System.currentTimeMillis;
import static java.lang.System.out;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.runners.MethodSorters.NAME_ASCENDING;

@Ignore
@RunWith(Parameterized.class)
@FixMethodOrder(NAME_ASCENDING)
public class PerformanceTest {

    @Parameters(name = "{index}: maxConnections={0}, threads={1}")
    public static Iterable<Object[]> data() {
        results = new TreeMap<>();
        List<Object[]> testData = new ArrayList<>();
        for (int poolSize = 1; poolSize <= 4; poolSize *= 2) {
            results.putIfAbsent(key(poolSize), new TreeMap<>());
            results.putIfAbsent(key(poolSize), new TreeMap<>());
            for (int threads = 1; threads <= 16; threads *= 2) {
                testData.add(new Object[]{poolSize, threads});
                testData.add(new Object[]{poolSize, threads});
            }
        }
        return testData;
    }

    private static String key(int poolSize) {
        return poolSize + " conn";
    }

    private static final int batchSize = 100;
    private static final int repeats = 5;
    private static SortedMap<String, SortedMap<Integer, Long>> results = new TreeMap<>();
    private final int poolSize;
    private final int numThreads;
    private final ConnectionPool pool;

    public PerformanceTest(int poolSize, int numThreads) {
        this.poolSize = poolSize;
        this.numThreads = numThreads;
        pool = DatabaseRule.createPoolBuilder(poolSize).build();
    }

    @After
    public void close() {
        pool.close();
    }

    @Test(timeout = 1000_0)
    public void t1_preAllocatePool() throws Exception {
        List<Connection> connections = new ArrayList<>();
        CompletableFuture.allOf((CompletableFuture<?>[]) IntStream.range(0, poolSize)
                .mapToObj(i -> pool.getConnection().thenAccept(connections::add))
                .toArray(size -> new CompletableFuture<?>[size])
        ).get();
        connections.forEach(Connection::close);
    }

    @Test
    public void t3_run() throws Exception {
        Collection<Callable<Long>> tasks = new ArrayList<>();
        for (int i = 0; i < batchSize; ++i) {
            tasks.add(new Callable<Long>() {
                final Exchanger<Long> swap = new Exchanger<>();

                @Override
                public Long call() throws Exception {
                    pool.completeQuery("select 42")
                            .thenAccept(r -> {
                                try {
                                    swap.exchange(currentTimeMillis());
                                } catch (Exception e) {
                                    throw new AssertionError(e);
                                }
                            })
                            .exceptionally(th -> {
                                throw new AssertionError(th);
                            });
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

        results.get(key(poolSize)).put(numThreads, minTime);

        out.printf("%d,%2d,%4.3f%n", poolSize, numThreads, minTime / 1000.0);
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