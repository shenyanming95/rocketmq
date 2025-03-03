package org.apache.rocketmq.srvutil;

import org.apache.rocketmq.logging.InternalLogger;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link ShutdownHookThread} is the standard hook for filtersrv and namesrv modules.
 * Through {@link Callable} interface, this hook can customization operations in anywhere.
 */
public class ShutdownHookThread extends Thread {
    private final InternalLogger log;
    private final Callable callback;
    private volatile boolean hasShutdown = false;
    private AtomicInteger shutdownTimes = new AtomicInteger(0);

    /**
     * Create the standard hook thread, with a call back, by using {@link Callable} interface.
     *
     * @param log      The log instance is used in hook thread.
     * @param callback The call back function.
     */
    public ShutdownHookThread(InternalLogger log, Callable callback) {
        super("ShutdownHook");
        this.log = log;
        this.callback = callback;
    }

    /**
     * Thread run method.
     * Invoke when the jvm shutdown.
     * 1. count the invocation times.
     * 2. execute the {@link ShutdownHookThread#callback}, and time it.
     */
    @Override
    public void run() {
        synchronized (this) {
            log.info("shutdown hook was invoked, " + this.shutdownTimes.incrementAndGet() + " times.");
            if (!this.hasShutdown) {
                this.hasShutdown = true;
                long beginTime = System.currentTimeMillis();
                try {
                    this.callback.call();
                } catch (Exception e) {
                    log.error("shutdown hook callback invoked failure.", e);
                }
                long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                log.info("shutdown hook done, consuming time total(ms): " + consumingTimeTotal);
            }
        }
    }
}
