package org.apache.rocketmq.client;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * Base interface for MQ management
 */
public interface MQAdmin {
    /**
     * Creates an topic
     *
     * @param key      accesskey
     * @param newTopic topic name
     * @param queueNum topic's queue number
     */
    void createTopic(final String key, final String newTopic, final int queueNum) throws MQClientException;

    /**
     * Creates an topic
     *
     * @param key          accesskey
     * @param newTopic     topic name
     * @param queueNum     topic's queue number
     * @param topicSysFlag topic system flag
     */
    void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException;

    /**
     * Gets the message queue offset according to some time in milliseconds<br>
     * be cautious to call because of more IO overhead
     *
     * @param mq        Instance of MessageQueue
     * @param timestamp from when in milliseconds.
     * @return offset
     */
    long searchOffset(final MessageQueue mq, final long timestamp) throws MQClientException;

    /**
     * Gets the max offset
     *
     * @param mq Instance of MessageQueue
     * @return the max offset
     */
    long maxOffset(final MessageQueue mq) throws MQClientException;

    /**
     * Gets the minimum offset
     *
     * @param mq Instance of MessageQueue
     * @return the minimum offset
     */
    long minOffset(final MessageQueue mq) throws MQClientException;

    /**
     * Gets the earliest stored message time
     *
     * @param mq Instance of MessageQueue
     * @return the time in microseconds
     */
    long earliestMsgStoreTime(final MessageQueue mq) throws MQClientException;

    /**
     * Query message according to message id
     *
     * @param offsetMsgId message id
     * @return message
     */
    MessageExt viewMessage(final String offsetMsgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    /**
     * Query messages
     *
     * @param topic  message topic
     * @param key    message key index word
     * @param maxNum max message number
     * @param begin  from when
     * @param end    to when
     * @return Instance of QueryResult
     */
    QueryResult queryMessage(final String topic, final String key, final int maxNum, final long begin, final long end) throws MQClientException, InterruptedException;

    /**
     * @return The {@code MessageExt} of given msgId
     */
    MessageExt viewMessage(String topic, String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

}