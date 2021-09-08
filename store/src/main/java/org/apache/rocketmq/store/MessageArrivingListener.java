package org.apache.rocketmq.store;

import java.util.Map;

/**
 * 消息到达监听器, 一般用于延时消息回调
 */
public interface MessageArrivingListener {
    void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties);
}
