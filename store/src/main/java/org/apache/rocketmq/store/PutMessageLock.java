package org.apache.rocketmq.store;

/**
 * Used when trying to put messageø
 */
public interface PutMessageLock {
    void lock();

    void unlock();
}
