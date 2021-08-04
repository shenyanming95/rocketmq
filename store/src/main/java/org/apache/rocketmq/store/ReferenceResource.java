package org.apache.rocketmq.store;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 引用计数, 类似netty的{@link io.netty.util.ReferenceCounted}
 */
public abstract class ReferenceResource {

    /**
     * 引用计数, 刚被创建, 那肯定值就为1
     */
    protected final AtomicLong refCount = new AtomicLong(1);

    /**
     * 标识该资源是否可用
     */
    protected volatile boolean available = true;

    /**
     * 标识该资源是否已经清理成功
     */
    protected volatile boolean cleanupOver = false;

    /**
     * 第一次关闭的时间戳, 即调用了{{@link #shutdown(long)}}方法
     */
    private volatile long firstShutdownTimestamp = 0;

    /**
     * 引用资源.
     * <p>
     * 将该资源的引用计数加1, 表示持有它.
     * 如果该资源已经不可用或者引用计算已经为0, 那么就无法持有
     *
     * @return true-持有资源成功
     */
    public synchronized boolean hold() {
        if (this.isAvailable()) {
            // 持有当前资源, 将引用计数加1
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                this.refCount.getAndDecrement();
            }
        }
        // 资源不可用, 或者, 它的引用计数小于等于0(意味着没有任何地方引用它了)
        return false;
    }

    /**
     * 释放资源
     */
    public void release() {
        // 引用计数减1
        long value = this.refCount.decrementAndGet();
        // 如果还大于0, 说明还有其它地方引用它, 因此不能作回收操作, 方法直接返回.
        if (value > 0) return;
        // 获取锁以后, 执行清除资源操作.
        synchronized (this) {
            this.cleanupOver = this.cleanup(value);
        }
    }

    /**
     * 关闭资源
     *
     * @param intervalForcibly 时间间隔
     */
    public void shutdown(final long intervalForcibly) {
        if (this.available) {
            // 如果资源目前还可以访问, 那么记录下它被关闭的时间, 然后调用 release() 方法释放资源.
            this.available = false;
            this.firstShutdownTimestamp = System.currentTimeMillis();
            this.release();
        } else if (this.getRefCount() > 0) {
            // 如果资源不可访问但是引用计数还大于0, 说明它之前被关闭过, 即调用过本方法.
            // 那么判断距离上次调用本方法的时间间隔是否大于等于参数指定的值, 如果是, 就再执行一次释放资源的操作.
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    public boolean isAvailable() {
        return this.available;
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }

    /**
     * 每种资源都有自己的清除方式, 子类需要自己实现.
     *
     * @param currentRef 本资源被引用的计数
     * @return true-清理成功
     */
    public abstract boolean cleanup(final long currentRef);
}
