package org.apache.rocketmq.store;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Spin lock Implementation to put message, suggest using this with low race conditions
 */
public class PutMessageSpinLock implements PutMessageLock {

    /**
     * 值为true, 表示可以加锁;
     * 值为false, 表示不可加锁.
     */
    private AtomicBoolean putMessageSpinLock = new AtomicBoolean(true);

    @Override
    public void lock() {
        boolean flag;
        do {
            // 只有 putMessageSpinLock 从true更新为false, 才表示加锁成功, 此时flag为true, 那么就会退出循环.
            // 否则说明 putMessageSpinLock 值为false, 此时flag为false, 就一直尝试获取.
            // 为了避免CPU一直在这里循环, 所以这个类一般用于低竞争条件下.
            flag = this.putMessageSpinLock.compareAndSet(true, false);
        } while (!flag);
    }

    @Override
    public void unlock() {
        this.putMessageSpinLock.compareAndSet(false, true);
    }
}
