package org.apache.rocketmq.common.consistenthash;

/**
 * Hash String to long value
 */
public interface HashFunction {
    long hash(String key);
}
