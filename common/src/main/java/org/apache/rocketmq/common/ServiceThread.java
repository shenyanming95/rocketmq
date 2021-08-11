package org.apache.rocketmq.common;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * rocketMQ 封装了{@link Runnable}, 抽象了通用方法.
 */
public abstract class ServiceThread implements Runnable {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    /**
     * {@link Thread#join(long)}的等待时间, 以毫秒为单位, 默认为90s
     */
    private static final long JOIN_TIME = 90 * 1000;

    /**
     * 在JDK自带的{@link java.util.concurrent.CountDownLatch}基础上增加了重置功能.
     */
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);

    /**
     * 标识是否已经启动线程, true：已启动
     */
    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * 标识是否已经唤醒
     */
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);

    /**
     * 标识是否已经停止线程运行
     */
    protected volatile boolean stopped = false;

    /**
     * 标识此线程是否是后台线程
     */
    protected boolean isDaemon = false;

    /**
     * 持有任务线程运行的句柄
     */
    private Thread thread;

    public ServiceThread() {

    }

    /**
     * 由子类实现, 用来标识此线程的业务名称
     *
     * @return 业务名称
     */
    public abstract String getServiceName();

    /**
     * 启动此线程
     */
    public void start() {
        log.info("Try to start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        // 首先通过CAS算法先将启动标识置为true
        if (!started.compareAndSet(false, true)) {
            return;
        }
        // 中止标识置为false
        stopped = false;
        // 创建一个新的线程句柄, 执行逻辑由子类实现
        this.thread = new Thread(this, getServiceName());
        this.thread.setDaemon(isDaemon);
        // 开启线程
        this.thread.start();
    }

    public void shutdown() {
        this.shutdown(false);
    }

    public void shutdown(final boolean interrupt) {
        log.info("Try to shutdown service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        // 首先使用CAS算法, 先将启动标识由true置为false.
        if (!started.compareAndSet(true, false)) {
            return;
        }
        // 中止标识置为false
        this.stopped = true;
        log.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);
        // 唤醒标识由false置为true
        if (hasNotified.compareAndSet(false, true)) {
            // notify
            waitPoint.countDown();
        }
        try {
            // 允许设置线程中断标识
            if (interrupt) {
                this.thread.interrupt();
            }
            long beginTime = System.currentTimeMillis();
            if (!this.thread.isDaemon()) {
                // 主线程(也就是调用这个方法的线程)将CPU让出来, 等待这个服务线程执行一段时间.
                this.thread.join(this.getJointime());
            }
            long elapsedTime = System.currentTimeMillis() - beginTime;
            log.info("join thread " + this.getServiceName() + " elapsed time(ms) " + elapsedTime + " " + this.getJointime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    public long getJointime() {
        return JOIN_TIME;
    }

    @Deprecated
    public void stop() {
        this.stop(false);
    }

    @Deprecated
    public void stop(final boolean interrupt) {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("stop thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        if (interrupt) {
            this.thread.interrupt();
        }
    }

    public void makeStop() {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("makestop thread " + this.getServiceName());
    }

    /**
     * 唤醒线程
     */
    public void wakeup() {
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }
    }

    protected void waitForRunning(long interval) {
        // 已经被唤醒过了, 直接调用唤醒后置处理逻辑, 然后方法返回, 就不需要阻塞了.
        if (hasNotified.compareAndSet(true, false)) {
            this.onWaitEnd();
            return;
        }
        // 重置, 就是将 java.util.concurrent.locks.AbstractQueuedSynchronizer.state 重新设置一开始的值
        waitPoint.reset();
        try {
            // 阻塞等待一段时间, 要么超时返回, 要么被唤醒返回, 甚至有可能假唤醒
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        } finally {
            // 阻塞状态结束后, 将 hasNotified 置为 false
            hasNotified.set(false);
            // 执行后置处理
            this.onWaitEnd();
        }
    }

    /**
     * 阻塞等待被唤醒的后置处理
     */
    protected void onWaitEnd() {
    }

    public boolean isStopped() {
        return stopped;
    }

    public boolean isDaemon() {
        return isDaemon;
    }

    public void setDaemon(boolean daemon) {
        isDaemon = daemon;
    }
}
