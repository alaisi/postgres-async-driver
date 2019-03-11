package com.github.pgasync;

import com.pgasync.Connectible;
import com.pgasync.Connection;
import com.pgasync.Listening;
import org.junit.After;
import org.junit.Before;
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

    private Connectible pool;

    @Before
    public void setup() {
        pool = dbr.pool();
    }

    @After
    public void shutdown() {
        pool.close().join();
    }

    @Test
    public void shouldReceiveNotificationsOnListenedChannel() throws InterruptedException {
        BlockingQueue<String> result = new LinkedBlockingQueue<>(5);

        Connection conn = pool.getConnection().join();
        try {
            Listening subscription = conn.subscribe("example", result::offer).join();
            try {
                TimeUnit.SECONDS.sleep(2);

                pool.completeScript("notify example, 'msg-1'").join();
                pool.completeScript("notify example, 'msg-2'").join();
                pool.completeScript("notify example, 'msg-3'").join();

                assertEquals("msg-1", result.poll(2, TimeUnit.SECONDS));
                assertEquals("msg-2", result.poll(2, TimeUnit.SECONDS));
                assertEquals("msg-3", result.poll(2, TimeUnit.SECONDS));
            } finally {
                subscription.unlisten().join();
            }
        } finally {
            conn.close().join();
        }

        pool.completeQuery("notify example, 'msg'").join();
        assertNull(result.poll(2, TimeUnit.SECONDS));
    }

}
