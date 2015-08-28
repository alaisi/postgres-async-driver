package com.github.pgasync.impl;

import com.github.pgasync.ConnectionPool;
import org.junit.AfterClass;
import org.junit.Test;

import java.util.concurrent.*;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Antti Laisi
 */
public class ListenNotifyTest {

    static final ConnectionPool pool = DatabaseRule.createPool(5);
    static final Consumer<Throwable> onError = Throwable::printStackTrace;

    @Test
    public void shouldReceiveNotificationsOnListenedChannel() throws Exception {
        BlockingQueue<String> result = new LinkedBlockingQueue<>(5);
        BlockingQueue<String> unlistenToken = new ArrayBlockingQueue<>(1);

        pool.listen("example", result::offer, unlistenToken::offer, onError);
        String token = unlistenToken.poll(2, TimeUnit.SECONDS);

        pool.query("notify example, 'msg'", rs -> { }, onError);
        pool.query("notify example, 'msg'", rs -> { }, onError);
        pool.query("notify example, 'msg'", rs -> { }, onError);

        assertEquals("msg", result.poll(2, TimeUnit.SECONDS));
        assertEquals("msg", result.poll(2, TimeUnit.SECONDS));
        assertEquals("msg", result.poll(2, TimeUnit.SECONDS));

        CountDownLatch unlistened = new CountDownLatch(1);
        pool.unlisten("example", token, unlistened::countDown, onError);
        assertTrue(unlistened.await(2, TimeUnit.SECONDS));
    }

    @AfterClass
    public static void close() {
        pool.close();
    }
}
