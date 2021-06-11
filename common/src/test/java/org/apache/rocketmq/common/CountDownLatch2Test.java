package org.apache.rocketmq.common;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * CountDownLatch2 Unit Test
 *
 * @see CountDownLatch2
 */
public class CountDownLatch2Test {

    /**
     * test constructor with invalid init param
     *
     * @see CountDownLatch2#CountDownLatch2(int)
     */
    @Test
    public void testConstructorError() {
        int count = -1;
        try {
            CountDownLatch2 latch = new CountDownLatch2(count);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("count < 0"));
        }
    }

    /**
     * test constructor with valid init param
     *
     * @see CountDownLatch2#CountDownLatch2(int)
     */
    @Test
    public void testConstructor() {
        int count = 10;
        CountDownLatch2 latch = new CountDownLatch2(count);
        assertEquals("Expected equal", count, latch.getCount());
        assertThat("Expected contain", latch.toString(), containsString("[Count = " + count + "]"));
    }

    /**
     * test await timeout
     *
     * @see CountDownLatch2#await(long, TimeUnit)
     */
    @Test
    public void testAwaitTimeout() throws InterruptedException {
        int count = 1;
        CountDownLatch2 latch = new CountDownLatch2(count);
        boolean await = latch.await(10, TimeUnit.MILLISECONDS);
        assertFalse("Expected false", await);

        latch.countDown();
        boolean await2 = latch.await(10, TimeUnit.MILLISECONDS);
        assertTrue("Expected true", await2);
    }


    /**
     * test reset
     *
     * @see CountDownLatch2#countDown()
     */
    @Test(timeout = 1000)
    public void testCountDownAndGetCount() throws InterruptedException {
        int count = 2;
        CountDownLatch2 latch = new CountDownLatch2(count);
        assertEquals("Expected equal", count, latch.getCount());
        latch.countDown();
        assertEquals("Expected equal", count - 1, latch.getCount());
        latch.countDown();
        latch.await();
        assertEquals("Expected equal", 0, latch.getCount());
    }


    /**
     * test reset
     *
     * @see CountDownLatch2#reset()
     */
    @Test
    public void testReset() throws InterruptedException {
        int count = 2;
        CountDownLatch2 latch = new CountDownLatch2(count);
        latch.countDown();
        assertEquals("Expected equal", count - 1, latch.getCount());
        latch.reset();
        assertEquals("Expected equal", count, latch.getCount());
        latch.countDown();
        latch.countDown();
        latch.await();
        assertEquals("Expected equal", 0, latch.getCount());
        // coverage Sync#tryReleaseShared, c==0
        latch.countDown();
        assertEquals("Expected equal", 0, latch.getCount());
    }
}
