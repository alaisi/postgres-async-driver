package com.github.pgasync.impl;

import com.github.pgasync.ConnectionPool;
import com.github.pgasync.ConnectionPoolBuilder;
import com.github.pgasync.Db;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import rx.Observable;
import rx.observers.TestSubscriber;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.github.pgasync.impl.DatabaseRule.createPoolBuilder;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TimeoutTest {
    private TestSubscriber testSubscriber;

    @ClassRule
    public static DatabaseRule dbr = new DatabaseRule(createPoolBuilder(1));

    @Before
    public void setup() {
        testSubscriber = TestSubscriber.create();
    }

    @Test
    public void shouldReportErrorOnReadTimeout() throws Exception {
        //given
        Db db = dbr.db();

        //when
        db.querySet("select pg_sleep(10)").timeout(1, TimeUnit.SECONDS).subscribe(testSubscriber);
        testSubscriber.awaitTerminalEvent();

        //then
        Object error = testSubscriber.getOnErrorEvents().get(0);
        assertThat(error, instanceOf(TimeoutException.class));
    }

    @Test
    public void shouldReportErrorWhenAttemptToNotExistingEndpoint() throws Exception {
        //given:
        int port = randomPort();
        ConnectionPool db = new ConnectionPoolBuilder().hostname("localhost")
                .port(port)
                .connectTimeout(1000)
                .build();

        //when
        db.querySet("select pg_sleep(5)").subscribe(testSubscriber);
        testSubscriber.awaitTerminalEvent();

        //then
        Throwable actual = (Throwable) testSubscriber.getOnErrorEvents().get(0);
        assertEquals("Connection refused: localhost/127.0.0.1:" + port, actual.getMessage());
    }

    @Test
    public void shouldReportErrorWhenAttemptToConnectToNotRespondingEndPoint() throws Exception {
        //given:
        int port = randomPort();
        try (Socket socket = createDummySocket("localhost", port)) {
            ConnectionPool db = new ConnectionPoolBuilder()
                    .hostname("localhost")
                    .port(port)
                    .connectTimeout(1000)
                    .build();
            //when
            db.querySet("select pg_sleep(5)").subscribe(testSubscriber);
            testSubscriber.awaitTerminalEvent();

            //then
            Throwable actual = (Throwable) testSubscriber.getOnErrorEvents().get(0);
            assertEquals("connection timed out: localhost/127.0.0.1:" + port, actual.getMessage());
        }
    }

    @Test
    public void shouldReconnectAfterFailure() throws Exception {
        //given
        Db db = dbr.db();

        //when
        Observable
                .interval(1500, TimeUnit.MILLISECONDS)
                .take(5)
                .map(i -> (i + 1) % 4)
                .flatMap(i ->
                        db.queryRows("SELECT pg_sleep(" + i + ")")
                                .timeout(2, TimeUnit.SECONDS)
                                .map(x -> "ok")
                                .onErrorReturn(e -> "error")
                )
                .subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();

        //then
        testSubscriber.assertValues("ok", "error", "error", "ok", "ok");
    }

    private Socket createDummySocket(String hostname, int port) throws IOException {
        Socket socket = new Socket();
        socket.bind(new InetSocketAddress(hostname, port));
        return socket;
    }

    private int randomPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }
}
