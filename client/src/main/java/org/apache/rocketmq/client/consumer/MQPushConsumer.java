package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;

/**
 * Push consumer
 */
public interface MQPushConsumer extends MQConsumer {
    /**
     * Start the consumer
     */
    void start() throws MQClientException;

    /**
     * Shutdown the consumer
     */
    void shutdown();

    /**
     * Register the message listener
     */
    @Deprecated
    void registerMessageListener(MessageListener messageListener);

    void registerMessageListener(final MessageListenerConcurrently messageListener);

    void registerMessageListener(final MessageListenerOrderly messageListener);

    /**
     * Subscribe some topic
     *
     * @param subExpression subscription expression.it only support or operation such as "tag1 || tag2 || tag3" <br> if
     *                      null or * expression,meaning subscribe
     *                      all
     */
    void subscribe(final String topic, final String subExpression) throws MQClientException;

    /**
     * This method will be removed in the version 5.0.0,because filterServer was removed,and method <code>subscribe(final String topic, final MessageSelector messageSelector)</code>
     * is recommended.
     * <p>
     * Subscribe some topic
     *
     * @param fullClassName     full class name,must extend org.apache.rocketmq.common.filter. MessageFilter
     * @param filterClassSource class source code,used UTF-8 file encoding,must be responsible for your code safety
     */
    @Deprecated
    void subscribe(final String topic, final String fullClassName, final String filterClassSource) throws MQClientException;

    /**
     * Subscribe some topic with selector.
     * <p>
     * This interface also has the ability of {@link #subscribe(String, String)},
     * and, support other message selection, such as {@link org.apache.rocketmq.common.filter.ExpressionType#SQL92}.
     * </p>
     * <p/>
     * <p>
     * Choose Tag: {@link MessageSelector#byTag(java.lang.String)}
     * </p>
     * <p/>
     * <p>
     * Choose SQL92: {@link MessageSelector#bySql(java.lang.String)}
     * </p>
     *
     * @param selector message selector({@link MessageSelector}), can be null.
     */
    void subscribe(final String topic, final MessageSelector selector) throws MQClientException;

    /**
     * Unsubscribe consumption some topic
     *
     * @param topic message topic
     */
    void unsubscribe(final String topic);

    /**
     * Update the consumer thread pool size Dynamically
     */
    void updateCorePoolSize(int corePoolSize);

    /**
     * Suspend the consumption
     */
    void suspend();

    /**
     * Resume the consumption
     */
    void resume();
}
