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

import com.github.pgasync.*;
import io.netty.handler.timeout.ReadTimeoutException;
import org.junit.*;
import rx.observers.TestSubscriber;

import java.io.IOException;
import java.net.*;

import static com.github.pgasync.impl.DatabaseRule.createPoolBuilder;
import static org.junit.Assert.assertEquals;

public class FailOverTest {

    private TestSubscriber testSubscriber;

    @ClassRule
    public static DatabaseRule dbr = new DatabaseRule(createPoolBuilder(1).readTimeout(2000));

    @Before
    public void setup() {
        testSubscriber = TestSubscriber.create();
    }

    @Test
    public void shouldReportErrorOnReadTimeout() throws Exception {
        //when
        dbr.db().querySet("select pg_sleep(600)").subscribe(testSubscriber);
        testSubscriber.awaitTerminalEvent();
        //then

        assertEquals(ReadTimeoutException.INSTANCE, testSubscriber.getOnErrorEvents().get(0));
    }

    @Test
    public void shouldReportErrorWhenAttemptToNotExistingEndpoint() throws Exception {
        //given:
        int port = randomPort();
        ConnectionPool db = new ConnectionPoolBuilder().hostname("localhost")
                .port(port)
                .connectTimeout(2000)
                .build();
        //when
        db.querySet("select pg_sleep(600)").subscribe(testSubscriber);
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
            ConnectionPool db = new ConnectionPoolBuilder().hostname("localhost")
                    .port(port)
                    .connectTimeout(2000)
                    .build();
            //when
            db.querySet("select pg_sleep(600)").subscribe(testSubscriber);
            testSubscriber.awaitTerminalEvent();

            //then
            Throwable actual = (Throwable) testSubscriber.getOnErrorEvents().get(0);
            assertEquals("connection timed out: localhost/127.0.0.1:" + port, actual.getMessage());
        }
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
