package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * RocketMQ 事务消息的处理器, 需要用户来实现.
 */
public interface TransactionListener {
    /**
     * 当Producer发送完Half消息后, 这个方法就会被回调, 用来执行本地事务, 然后返回本地事务执行结果.
     * 如果是{@link LocalTransactionState#COMMIT_MESSAGE}则Producer自动向Broker发送事务提交请求,
     * 如果是{@link LocalTransactionState#ROLLBACK_MESSAGE}则Producer自动向Broker发送事务回滚请求.
     *
     * @param msg Half(prepare) 消息
     * @param arg 用户自定义参数
     * @return 本地事务执行结果
     */
    LocalTransactionState executeLocalTransaction(final Message msg, final Object arg);

    /**
     * 当Producer发送完Half消息后, 未给Broker再发送commit或rollback消息, broker就会主动向Producer发起请求, 检查本地事务的执行情况.
     *
     * @param msg Broker发来的消息
     * @return 本地事务执行结果
     */
    LocalTransactionState checkLocalTransaction(final MessageExt msg);
}