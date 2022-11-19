package org.apache.rocketmq.broker.longpolling;

import io.netty.channel.Channel;
import org.apache.rocketmq.client.consumer.MQPullConsumer;
import org.apache.rocketmq.client.consumer.MQPushConsumer;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageFilter;

/**
 * 一般MQ针对Consumer有两种方式的消息通知：PUSH、PULL.
 * PUSH 模式由 Broker 主动推给 Consumer; PULL 模式由 Consumer 主动拉取 消息.
 * 但是在 RocketMQ 中, 虽然两种都支持, 但是它的实现只有 PULL 模式, PUSH 是基于 PULL 模式实现的.
 *
 * {@link PullRequest}就是针对 PULL 模式的请求封装.
 *
 * @see MQPullConsumer
 * @see MQPushConsumer
 */
public class PullRequest {

    /**
     * 客户端请求
     */
    private final RemotingCommand requestCommand;

    /**
     * TCP通道
     */
    private final Channel clientChannel;

    private final long timeoutMillis;
    private final long suspendTimestamp;
    private final long pullFromThisOffset;
    private final SubscriptionData subscriptionData;
    private final MessageFilter messageFilter;

    public PullRequest(RemotingCommand requestCommand, Channel clientChannel, long timeoutMillis, long suspendTimestamp, long pullFromThisOffset, SubscriptionData subscriptionData, MessageFilter messageFilter) {
        this.requestCommand = requestCommand;
        this.clientChannel = clientChannel;
        this.timeoutMillis = timeoutMillis;
        this.suspendTimestamp = suspendTimestamp;
        this.pullFromThisOffset = pullFromThisOffset;
        this.subscriptionData = subscriptionData;
        this.messageFilter = messageFilter;
    }

    public RemotingCommand getRequestCommand() {
        return requestCommand;
    }

    public Channel getClientChannel() {
        return clientChannel;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public long getSuspendTimestamp() {
        return suspendTimestamp;
    }

    public long getPullFromThisOffset() {
        return pullFromThisOffset;
    }

    public SubscriptionData getSubscriptionData() {
        return subscriptionData;
    }

    public MessageFilter getMessageFilter() {
        return messageFilter;
    }
}
