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

import com.pgasync.Connection;
import com.pgasync.Connectible;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.github.pgasync.DatabaseRule.createPoolBuilder;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.out;

@RunWith(Parameterized.class)
public class PerformanceTest {

    private static DatabaseRule dbr;

    static {
        System.setProperty("io.netty.eventLoopThreads", "1");
        dbr = new DatabaseRule(createPoolBuilder(1));
    }

    private static final String SELECT_42 = "select 42";


    @Parameters(name = "{index}: maxConnections={0}, threads={1}")
    public static Iterable<Object[]> data() {
        List<Object[]> testData = new ArrayList<>();
        for (int poolSize = 1; poolSize <= 8; poolSize *= 2) {
            for (int threads = 1; threads <= 8; threads *= 2) {
                testData.add(new Object[]{poolSize, threads});
            }
        }
        return testData;
    }

    private static final int batchSize = 1000;
    private static final int repeats = 5;
    private static SortedMap<Integer, SortedMap<Integer, Long>> simpleQueryResults = new TreeMap<>();
    private static SortedMap<Integer, SortedMap<Integer, Long>> preparedStatementResults = new TreeMap<>();

    private final int poolSize;
    private final int numThreads;
    private Connectible pool;

    public PerformanceTest(int poolSize, int numThreads) {
        this.poolSize = poolSize;
        this.numThreads = numThreads;
    }


    @Before
    public void setup() {
        pool = dbr.builder
                .password("async-pg")
                .maxConnections(poolSize)
                .pool(Executors.newFixedThreadPool(numThreads));
        List<Connection> connections = IntStream.range(0, poolSize)
                .mapToObj(i -> pool.getConnection().join()).collect(Collectors.toList());
        connections.forEach(connection -> {
            connection.prepareStatement(SELECT_42).join().close().join();
            connection.close().join();
        });
    }

    @After
    public void tearDown() {
        pool.close().join();
    }

    @Test
    public void observeBatches() {
        performBatches(simpleQueryResults, i -> new Batch(batchSize).startWithSimpleQuery());
        performBatches(preparedStatementResults, i -> new Batch(batchSize).startWithPreparedStatement());
    }

    private void performBatches(SortedMap<Integer, SortedMap<Integer, Long>> results, IntFunction<CompletableFuture<Long>> batchStarter) {
        double mean = LongStream.range(0, repeats)
                .map(i -> {
                    try {
                        List<CompletableFuture<Long>> batches = IntStream.range(0, poolSize)
                                .mapToObj(batchStarter)
                                .collect(Collectors.toList());
                        CompletableFuture.allOf(batches.toArray(new CompletableFuture<?>[]{})).get();
                        return batches.stream().map(CompletableFuture::join).max(Long::compare).get();
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                })
                .average().getAsDouble();
        results.computeIfAbsent(poolSize, k -> new TreeMap<>())
                .put(numThreads, Math.round(mean));
    }

    private class Batch {

        private long batchSize;
        private long performed;
        private long startedAt;
        private CompletableFuture<Long> onBatch;

        Batch(long batchSize) {
            this.batchSize = batchSize;
        }

        private CompletableFuture<Long> startWithPreparedStatement() {
            onBatch = new CompletableFuture<>();
            startedAt = System.currentTimeMillis();
            nextSamplePreparedStatement();
            return onBatch;
        }

        private CompletableFuture<Long> startWithSimpleQuery() {
            onBatch = new CompletableFuture<>();
            startedAt = System.currentTimeMillis();
            nextSampleSimpleQuery();
            return onBatch;
        }

        private void nextSamplePreparedStatement() {
            pool.getConnection()
                    .thenApply(connection ->
                            connection.prepareStatement(SELECT_42)
                                    .thenApply(stmt -> stmt.query()
                                            .thenApply(rs -> stmt.close())
                                            .exceptionally(th -> stmt.close().whenComplete((v, _th) -> {
                                                throw new RuntimeException(th);
                                            }))
                                            .thenCompose(Function.identity())
                                            .thenApply(v -> connection.close())
                                            .exceptionally(th -> connection.close().whenComplete((v, _th) -> {
                                                throw new RuntimeException(th);
                                            }))
                                            .thenCompose(Function.identity())
                                    )
                                    .thenCompose(Function.identity())
                    )
                    .thenCompose(Function.identity())
                    .thenAccept(v -> {
                        if (++performed < batchSize) {
                            nextSamplePreparedStatement();
                        } else {
                            long duration = currentTimeMillis() - startedAt;
                            onBatch.complete(duration);
                        }
                    })
                    .exceptionally(th -> {
                        onBatch.completeExceptionally(th);
                        return null;
                    });

        }

        private void nextSampleSimpleQuery() {
            pool.completeScript(SELECT_42)
                    .thenAccept(v -> {
                        if (++performed < batchSize) {
                            nextSamplePreparedStatement();
                        } else {
                            long duration = currentTimeMillis() - startedAt;
                            onBatch.complete(duration);
                        }
                    })
                    .exceptionally(th -> {
                        onBatch.completeExceptionally(th);
                        return null;
                    });
        }
    }

    @AfterClass
    public static void printResults() {
        out.println();
        out.println("Requests per second, Hz:");
        out.println();
        out.println("Simple query protocol");
        printResults(simpleQueryResults);
        out.println();
        out.println("Extended query protocol (reusing prepared statement)");
        printResults(preparedStatementResults);
    }

    private static void printResults(SortedMap<Integer, SortedMap<Integer, Long>> results) {
        out.print(" threads");
        results.keySet().forEach(i -> out.printf("\t%d conn\t", i));
        out.println();

        results.values().iterator().next().keySet().forEach(threads -> {
            out.print("    " + threads);
            results.keySet().forEach(connections -> {
                long batchDuration = results.get(connections).get(threads);
                double rps = 1000 * batchSize * connections / (double) batchDuration;
                out.printf("\t\t%d", Math.round(rps));
            });
            out.println();
        });
    }
}