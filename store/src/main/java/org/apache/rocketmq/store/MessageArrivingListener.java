package org.apache.rocketmq.store;

import java.util.Map;

/**
 * 消息到达监听器, 在 commitLog 数据写入时执行回调,
 * 用于实现MQ的PULL模式消息拉取.
 */
public interface MessageArrivingListener {
    void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties);
}
