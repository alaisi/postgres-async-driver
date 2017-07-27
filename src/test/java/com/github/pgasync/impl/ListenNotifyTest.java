package com.github.pgasync.impl;

import com.github.pgasync.ConnectionPool;
import io.reactivex.disposables.Disposable;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author Antti Laisi
 */
public class ListenNotifyTest {

    @ClassRule
    public static DatabaseRule dbr = new DatabaseRule(DatabaseRule.createPoolBuilder(5));

    @Test
    public void shouldReceiveNotificationsOnListenedChannel() throws Exception {
        ConnectionPool pool = dbr.pool;
        BlockingQueue<String> result = new LinkedBlockingQueue<>(5);

        Disposable subscription = pool.listen("example").subscribe(result::add, Throwable::printStackTrace);
        TimeUnit.SECONDS.sleep(2);

        pool.querySet("notify example, 'msg'").blockingGet();
        pool.querySet("notify example, 'msg'").blockingGet();
        pool.querySet("notify example, 'msg'").blockingGet();

        assertEquals("msg", result.poll(2, TimeUnit.SECONDS));
        assertEquals("msg", result.poll(2, TimeUnit.SECONDS));
        assertEquals("msg", result.poll(2, TimeUnit.SECONDS));

        subscription.dispose();
        assertTrue(subscription.isDisposed());

        pool.querySet("notify example, 'msg'").blockingGet();
        assertNull(result.poll(2, TimeUnit.SECONDS));
    }
}
