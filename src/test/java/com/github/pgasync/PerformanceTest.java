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

import com.pgasync.ConnectionPool;
import com.pgasync.PreparedStatement;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.github.pgasync.DatabaseRule.createPoolBuilder;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.out;
import static org.junit.runners.MethodSorters.NAME_ASCENDING;

@RunWith(Parameterized.class)
@FixMethodOrder(NAME_ASCENDING)
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
            for (int threads = 1; threads <= 16; threads *= 2) {
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
        pool = dbr.builder
                .password("async-pg")
                .maxConnections(poolSize)
                .build();
    }

    @After
    public void close() {
        pool.close();
    }

    @Test(timeout = 2000)
    public void t1_preAllocatePool() throws Exception {
        CompletableFuture.allOf((CompletableFuture<?>[]) IntStream.range(0, poolSize)
                .mapToObj(i -> pool.getConnection()
                        .thenApply(connection ->
                                connection.prepareStatement(SELECT_42)
                                        .thenApply(PreparedStatement::close)
                                        .thenCompose(Function.identity())
                                        .thenApply(v -> connection.close())
                                        .thenCompose(Function.identity())
                        )
                        .thenCompose(Function.identity())
                )
                .toArray(size -> new CompletableFuture<?>[size])
        ).get();
    }

    @Test
    public void t3_run() {
        double mean = LongStream.range(0, repeats)
                .map(i -> {
                    try {
                        return performBatch();
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                })
                .average().getAsDouble();
        results.computeIfAbsent(poolSize + " conn", k -> new TreeMap<>())
                .put(numThreads, Math.round(mean));
    }

    private long performBatch() throws Exception {
        List<CompletableFuture<Void>> batchFutures = new ArrayList<>();
        long startTime = currentTimeMillis();
        for (int i = 0; i < batchSize; i++) {

            batchFutures.add(pool.getConnection()
                    .thenApply(connection -> connection.prepareStatement(SELECT_42)
                            .thenApply(stmt -> {
                                        return stmt.query()
                                                .handle((v, th) ->
                                                        stmt.close()
                                                                .thenAccept(_v -> {
                                                                    if (th != null) {
                                                                        throw new RuntimeException(th);
                                                                    }
                                                                })
                                                )
                                                .thenCompose(Function.identity());
                                    }
                            )
                            .thenCompose(Function.identity())
                            .handle((v, th) -> connection.close()
                                    .thenAccept(_v -> {
                                        if (th != null) {
                                            throw new RuntimeException(th);
                                        }
                                    }))
                            .thenCompose(Function.identity())
                    )
                    .thenCompose(Function.identity())
                    .exceptionally(th -> {
                        throw new AssertionError(th);
                    }));

/*
            batchFutures.add(pool.completeScript("select 42").thenAccept(rs -> {
            }));
            */
        }
        CompletableFuture
                .allOf(batchFutures.toArray(new CompletableFuture<?>[]{}))
                .get();
        long duration = currentTimeMillis() - startTime;
        return duration;
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