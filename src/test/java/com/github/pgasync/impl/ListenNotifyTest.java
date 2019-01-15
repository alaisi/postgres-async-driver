package com.github.pgasync.impl;

import com.github.pgasync.ConnectionPool;
import com.github.pgasync.Listening;
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

        Listening subscription = pool.getConnection().get().subscribe("example", result::offer).get();
        try {
            TimeUnit.SECONDS.sleep(2);

            pool.completeScript("notify example, 'msg-1'").get();
            pool.completeScript("notify example, 'msg-2'").get();
            pool.completeScript("notify example, 'msg-3'").get();

            assertEquals("msg-1", result.poll(2, TimeUnit.SECONDS));
            assertEquals("msg-2", result.poll(2, TimeUnit.SECONDS));
            assertEquals("msg-3", result.poll(2, TimeUnit.SECONDS));
        } finally {
            subscription.unlisten();
        }
        pool.completeQuery("notify example, 'msg'").get();
        assertNull(result.poll(2, TimeUnit.SECONDS));
    }

}
