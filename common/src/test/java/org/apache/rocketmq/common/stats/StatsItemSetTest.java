package org.apache.rocketmq.common.stats;

import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class StatsItemSetTest {

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private ThreadPoolExecutor executor;

    @Test
    public void test_getAndCreateStatsItem_multiThread() throws InterruptedException {
        assertEquals(20L, test_unit().longValue());
    }

    @Test
    public void test_getAndCreateMomentStatsItem_multiThread() throws InterruptedException {
        assertEquals(10, test_unit_moment().longValue());
    }

    @Test
    public void test_statsOfFirstStatisticsCycle() throws InterruptedException {
        final String tpsStatKey = "tpsTest";
        final String rtStatKey = "rtTest";
        final StatsItemSet statsItemSet = new StatsItemSet(tpsStatKey, scheduler, null);
        executor = new ThreadPoolExecutor(10, 20, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(100), new ThreadFactoryImpl("testMultiThread"));
        for (int i = 0; i < 10; i++) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    statsItemSet.addValue(tpsStatKey, 2, 1);
                    statsItemSet.addRTValue(rtStatKey, 2, 1);
                }
            });
        }
        while (true) {
            if (executor.getCompletedTaskCount() == 10) {
                break;
            }
            Thread.sleep(1000);
        }
        // simulate schedule task execution , tps stat
        {
            statsItemSet.getStatsItem(tpsStatKey).samplingInSeconds();
            statsItemSet.getStatsItem(tpsStatKey).samplingInMinutes();
            statsItemSet.getStatsItem(tpsStatKey).samplingInHour();

            assertEquals(20L, statsItemSet.getStatsDataInMinute(tpsStatKey).getSum());
            assertEquals(20L, statsItemSet.getStatsDataInHour(tpsStatKey).getSum());
            assertEquals(20L, statsItemSet.getStatsDataInDay(tpsStatKey).getSum());
            assertEquals(10L, statsItemSet.getStatsDataInDay(tpsStatKey).getTimes());
            assertEquals(10L, statsItemSet.getStatsDataInHour(tpsStatKey).getTimes());
            assertEquals(10L, statsItemSet.getStatsDataInDay(tpsStatKey).getTimes());
        }

        // simulate schedule task execution , rt stat
        {
            statsItemSet.getStatsItem(rtStatKey).samplingInSeconds();
            statsItemSet.getStatsItem(rtStatKey).samplingInMinutes();
            statsItemSet.getStatsItem(rtStatKey).samplingInHour();

            assertEquals(20L, statsItemSet.getStatsDataInMinute(rtStatKey).getSum());
            assertEquals(20L, statsItemSet.getStatsDataInHour(rtStatKey).getSum());
            assertEquals(20L, statsItemSet.getStatsDataInDay(rtStatKey).getSum());
            assertEquals(10L, statsItemSet.getStatsDataInDay(rtStatKey).getTimes());
            assertEquals(10L, statsItemSet.getStatsDataInHour(rtStatKey).getTimes());
            assertEquals(10L, statsItemSet.getStatsDataInDay(rtStatKey).getTimes());
        }
    }

    private AtomicLong test_unit() throws InterruptedException {
        final StatsItemSet statsItemSet = new StatsItemSet("topicTest", scheduler, null);
        executor = new ThreadPoolExecutor(10, 20, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(100), new ThreadFactoryImpl("testMultiThread"));
        for (int i = 0; i < 10; i++) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    statsItemSet.addValue("topicTest", 2, 1);
                }
            });
        }
        while (true) {
            if (executor.getCompletedTaskCount() == 10) {
                break;
            }
            Thread.sleep(1000);
        }
        return statsItemSet.getStatsItem("topicTest").getValue();
    }

    private AtomicLong test_unit_moment() throws InterruptedException {
        final MomentStatsItemSet statsItemSet = new MomentStatsItemSet("topicTest", scheduler, null);
        executor = new ThreadPoolExecutor(10, 20, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(100), new ThreadFactoryImpl("testMultiThread"));
        for (int i = 0; i < 10; i++) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    statsItemSet.setValue("test", 10);
                }
            });
        }
        while (true) {
            if (executor.getCompletedTaskCount() == 10) {
                break;
            }
            Thread.sleep(1000);
        }
        return statsItemSet.getAndCreateStatsItem("test").getValue();
    }

    @After
    public void shutdown() {
        executor.shutdown();
    }
}