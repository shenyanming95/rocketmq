package org.apache.rocketmq.store.ha;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 用来实现{@link Object#wait()}和{@link Object#notifyAll()}
 */
public class WaitNotifyObject {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 线程是否已经被唤醒
     */
    protected final HashMap<Long/* thread id */, Boolean/* notified */> waitingThreadTable = new HashMap<Long, Boolean>(16);

    /**
     * 标识这个类实例是否被唤醒过, 用于{@link #wakeup()}和{@link #waitForRunning(long)}
     */
    protected volatile boolean hasNotified = false;

    /**
     * 唤醒阻塞在这个类的线程, 它只会调用{@link Object#notify()},
     * 因此只会唤醒一个线程.
     */
    public void wakeup() {
        synchronized (this) {
            // 未被唤醒的话, 直接唤醒
            if (!this.hasNotified) {
                this.hasNotified = true;
                // 调用 java.lang.Object.notify() 方法
                this.notify();
            }
        }
    }

    /**
     * 阻塞当前线程指定的时间, 如果该类已经执行过唤醒操作, 那么会立即返回.
     * 这个方法区别于{@link #allWaitForRunning(long)}, 他不会把当前线程加入到集合中.
     *
     * @param interval 阻塞的时间
     */
    protected void waitForRunning(long interval) {
        synchronized (this) {
            // 这个对象已经执行过唤醒操作, 方法立即返回
            if (this.hasNotified) {
                this.hasNotified = false;
                this.onWaitEnd();
                return;
            }

            try {
                // 阻塞一段时间
                this.wait(interval);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            } finally {
                this.hasNotified = false;
                this.onWaitEnd();
            }
        }
    }

    /**
     * 唤醒后的回调, 空实现.
     */
    protected void onWaitEnd() {
    }

    /**
     * 唤醒所有阻塞在这个类的线程
     */
    public void wakeupAll() {
        synchronized (this) {
            // 是否需要唤醒
            boolean needNotify = false;

            // 遍历集合, 只要有一个线程未被唤醒, 最终都会执行全部唤醒
            for (Map.Entry<Long, Boolean> entry : this.waitingThreadTable.entrySet()) {
                // 只要有一个线程未被唤醒, 即entry.getValue() == false, 最终就会执行全部唤醒
                needNotify = needNotify || !entry.getValue();
                // 重置所有线程的唤醒标识为已唤醒
                entry.setValue(true);
            }
            // 唤醒阻塞在这个类的所有线程
            if (needNotify) {
                this.notifyAll();
            }
        }
    }

    /**
     * 阻塞当前线程指定的时间, 如果当前线程已经被唤醒过, 那么方法会立即返回
     *
     * @param interval 阻塞的时间
     */
    public void allWaitForRunning(long interval) {
        // 获取当前运行线程
        long currentThreadId = Thread.currentThread().getId();
        synchronized (this) {
            // 如果线程存在于集合中, 并且已经被唤醒过了, 那么更新线程唤醒状态为false, 方法直接返回
            Boolean notified = this.waitingThreadTable.get(currentThreadId);
            if (notified != null && notified) {
                this.waitingThreadTable.put(currentThreadId, false);
                this.onWaitEnd();
                return;
            }

            try {
                // 调用 java.lang.Object.wait() 阻塞一段时间
                this.wait(interval);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            } finally {
                // 被唤醒后, 或者时间到以后, 更新线程阻塞状态为false, 方法立即返回
                this.waitingThreadTable.put(currentThreadId, false);
                this.onWaitEnd();
            }
        }
    }

    /**
     * 将当前线程从维护的{@link #waitingThreadTable}删除
     */
    public void removeFromWaitingThreadTable() {
        // 获取当前线程id, 将其移除
        long currentThreadId = Thread.currentThread().getId();
        synchronized (this) {
            this.waitingThreadTable.remove(currentThreadId);
        }
    }
}
