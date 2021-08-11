package org.apache.rocketmq.store;

/**
 * 存储消息的写锁
 */
public interface PutMessageLock {

    void lock();

    void unlock();
}
