package org.apache.rocketmq.common;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * 在{@link CountDownLatch}基础增加重置功能
 */
public class CountDownLatch2 {

    private static final class Sync extends AbstractQueuedSynchronizer {
        private static final long serialVersionUID = 4982264981922014374L;

        /**
         * 在创建这个类的时候, 就将参数存储到这里, 后续调用
         * {@link #reset()}方法将{@link AbstractQueuedSynchronizer#state}重新置为起始值,
         * 这样就可以达到重置的效果
         */
        private final int startCount;

        Sync(int count) {
            this.startCount = count;
            setState(count);
        }

        int getCount() {
            return getState();
        }

        protected int tryAcquireShared(int acquires) {
            return (getState() == 0) ? 1 : -1;
        }

        protected boolean tryReleaseShared(int releases) {
            // Decrement count; signal when transition to zero
            for (; ; ) {
                int c = getState();
                if (c == 0) return false;
                int nextc = c - 1;
                if (compareAndSetState(c, nextc)) return nextc == 0;
            }
        }

        /**
         * rocketMQ 新增的方法
         */
        protected void reset() {
            setState(startCount);
        }
    }

    private final Sync sync;

    public CountDownLatch2(int count) {
        if (count < 0) throw new IllegalArgumentException("count < 0");
        this.sync = new Sync(count);
    }

    public void await() throws InterruptedException {
        sync.acquireSharedInterruptibly(1);
    }

    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return sync.tryAcquireSharedNanos(1, unit.toNanos(timeout));
    }

    public void countDown() {
        sync.releaseShared(1);
    }

    public long getCount() {
        return sync.getCount();
    }

    /**
     * rocketMQ新增的方法
     */
    public void reset() {
        sync.reset();
    }

    public String toString() {
        return super.toString() + "[Count = " + sync.getCount() + "]";
    }

}
