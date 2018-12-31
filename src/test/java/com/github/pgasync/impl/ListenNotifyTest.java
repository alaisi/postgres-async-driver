package com.github.pgasync.impl;

import com.github.pgasync.ConnectionPool;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author Antti Laisi
 */
@Ignore("Until subscription addition")
public class ListenNotifyTest {

    @ClassRule
    public static DatabaseRule dbr = new DatabaseRule(DatabaseRule.createPoolBuilder(5));
/*
    @Test
    public void shouldReceiveNotificationsOnListenedChannel() throws Exception {
        ConnectionPool pool = dbr.pool;
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
    */
}
