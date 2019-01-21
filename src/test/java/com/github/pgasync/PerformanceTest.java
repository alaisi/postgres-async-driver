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
import com.pgasync.ConnectionPool;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.github.pgasync.DatabaseRule.createPoolBuilder;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.out;

@RunWith(Parameterized.class)
public class PerformanceTest {

    private static final String SELECT_42 = "select 42";

    @ClassRule
    public static DatabaseRule dbr = new DatabaseRule(createPoolBuilder(1));

    @Parameters(name = "{index}: maxConnections={0}, threads={1}")
    public static Iterable<Object[]> data() {
        results = new TreeMap<>();
        List<Object[]> testData = new ArrayList<>();
        for (int poolSize = 1; poolSize <= 4; poolSize *= 2) {
            results.putIfAbsent(key(poolSize), new TreeMap<>());
            for (int threads = 1; threads <= 1; threads *= 2) {
                testData.add(new Object[]{poolSize, threads});
            }
        }
        return testData;
    }

    private static String key(int poolSize) {
        return poolSize + " conn";
    }

    private static final int batchSize = 1000;
    private static final int repeats = 5;
    private static SortedMap<String, SortedMap<Integer, Long>> results = new TreeMap<>();

    private final int poolSize;
    private final int numThreads;
    private ConnectionPool pool;

    public PerformanceTest(int poolSize, int numThreads) {
        this.poolSize = poolSize;
        this.numThreads = numThreads;
    }

    @Before
    public void setup() {
        pool = dbr.builder
                .password("async-pg")
                .maxConnections(poolSize)
                .build();
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
    public void observeSomeBatches() {
        double mean = LongStream.range(0, repeats)
                .map(i -> {
                    try {
                        return new Batch(batchSize).perform().get();
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                })
                .average().getAsDouble();
        results.computeIfAbsent(poolSize + " conn", k -> new TreeMap<>())
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

        private CompletableFuture<Long> perform() {
            onBatch = new CompletableFuture<>();
            startedAt = System.currentTimeMillis();
            nextSample();
            return onBatch;
        }

        private void nextSample() {
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
                            nextSample();
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
        out.print(" threads");
        results.keySet().forEach(i -> out.printf("\t%s\t", i));
        out.println();

        results.values().iterator().next().keySet().forEach(threads -> {
            out.print("    " + threads);
            results.keySet().forEach(connections -> {
                long batchDuration = results.get(connections).get(threads);
                double rps = 1000 * batchSize / (double) batchDuration;
                out.printf("\t\t%d", Math.round(rps));
            });
            out.println();
        });
    }
}