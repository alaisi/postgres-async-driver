package com.github.pgasync.impl;

import com.github.pgasync.ConnectionPool;
import org.junit.AfterClass;
import org.junit.Test;
import rx.Subscription;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Antti Laisi
 */
public class ListenNotifyTest {

    static final ConnectionPool pool = DatabaseRule.createPool(5);

    @Test
    public void shouldReceiveNotificationsOnListenedChannel() throws Exception {
        BlockingQueue<String> result = new LinkedBlockingQueue<>(5);

        Subscription subscription = pool.listen("example").subscribe(result::add, Throwable::printStackTrace);
        TimeUnit.SECONDS.sleep(2);

        pool.querySet("notify example, 'msg'").toBlocking().single();
        pool.querySet("notify example, 'msg'").toBlocking().single();
        pool.querySet("notify example, 'msg'").toBlocking().single();

        assertEquals("msg", result.poll(2, TimeUnit.SECONDS));
        assertEquals("msg", result.poll(2, TimeUnit.SECONDS));
        assertEquals("msg", result.poll(2, TimeUnit.SECONDS));

        subscription.unsubscribe();
        assertTrue(subscription.isUnsubscribed());

        pool.querySet("notify example, 'msg'").toBlocking().single();
        assertNull(result.poll(2, TimeUnit.SECONDS));
    }

    @AfterClass
    public static void close() {
        pool.close().toBlocking().single();
    }
}
