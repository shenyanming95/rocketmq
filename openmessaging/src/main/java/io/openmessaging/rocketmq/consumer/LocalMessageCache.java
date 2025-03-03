package io.openmessaging.rocketmq.consumer;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.ServiceLifecycle;
import io.openmessaging.rocketmq.config.ClientConfig;
import io.openmessaging.rocketmq.domain.ConsumeRequest;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;

class LocalMessageCache implements ServiceLifecycle {
    private final static InternalLogger log = ClientLogger.getLog();
    private final BlockingQueue<ConsumeRequest> consumeRequestCache;
    private final Map<String, ConsumeRequest> consumedRequest;
    private final ConcurrentHashMap<MessageQueue, Long> pullOffsetTable;
    private final DefaultMQPullConsumer rocketmqPullConsumer;
    private final ClientConfig clientConfig;
    private final ScheduledExecutorService cleanExpireMsgExecutors;

    LocalMessageCache(final DefaultMQPullConsumer rocketmqPullConsumer, final ClientConfig clientConfig) {
        consumeRequestCache = new LinkedBlockingQueue<>(clientConfig.getRmqPullMessageCacheCapacity());
        this.consumedRequest = new ConcurrentHashMap<>();
        this.pullOffsetTable = new ConcurrentHashMap<>();
        this.rocketmqPullConsumer = rocketmqPullConsumer;
        this.clientConfig = clientConfig;
        this.cleanExpireMsgExecutors = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("OMS_CleanExpireMsgScheduledThread_"));
    }

    int nextPullBatchNums() {
        return Math.min(clientConfig.getRmqPullMessageBatchNums(), consumeRequestCache.remainingCapacity());
    }

    long nextPullOffset(MessageQueue remoteQueue) {
        if (!pullOffsetTable.containsKey(remoteQueue)) {
            try {
                pullOffsetTable.putIfAbsent(remoteQueue, rocketmqPullConsumer.fetchConsumeOffset(remoteQueue, false));
            } catch (MQClientException e) {
                log.error("An error occurred in fetch consume offset process.", e);
            }
        }
        return pullOffsetTable.get(remoteQueue);
    }

    void updatePullOffset(MessageQueue remoteQueue, long nextPullOffset) {
        pullOffsetTable.put(remoteQueue, nextPullOffset);
    }

    void submitConsumeRequest(ConsumeRequest consumeRequest) {
        try {
            consumeRequestCache.put(consumeRequest);
        } catch (InterruptedException ignore) {
        }
    }

    MessageExt poll() {
        return poll(clientConfig.getOperationTimeout());
    }

    MessageExt poll(final KeyValue properties) {
        int currentPollTimeout = clientConfig.getOperationTimeout();
        if (properties.containsKey(Message.BuiltinKeys.TIMEOUT)) {
            currentPollTimeout = properties.getInt(Message.BuiltinKeys.TIMEOUT);
        }
        return poll(currentPollTimeout);
    }

    private MessageExt poll(long timeout) {
        try {
            ConsumeRequest consumeRequest = consumeRequestCache.poll(timeout, TimeUnit.MILLISECONDS);
            if (consumeRequest != null) {
                MessageExt messageExt = consumeRequest.getMessageExt();
                consumeRequest.setStartConsumeTimeMillis(System.currentTimeMillis());
                MessageAccessor.setConsumeStartTimeStamp(messageExt, String.valueOf(consumeRequest.getStartConsumeTimeMillis()));
                consumedRequest.put(messageExt.getMsgId(), consumeRequest);
                return messageExt;
            }
        } catch (InterruptedException ignore) {
        }
        return null;
    }

    void ack(final String messageId) {
        ConsumeRequest consumeRequest = consumedRequest.remove(messageId);
        if (consumeRequest != null) {
            long offset = consumeRequest.getProcessQueue().removeMessage(Collections.singletonList(consumeRequest.getMessageExt()));
            try {
                rocketmqPullConsumer.updateConsumeOffset(consumeRequest.getMessageQueue(), offset);
            } catch (MQClientException e) {
                log.error("An error occurred in update consume offset process.", e);
            }
        }
    }

    void ack(final MessageQueue messageQueue, final ProcessQueue processQueue, final MessageExt messageExt) {
        consumedRequest.remove(messageExt.getMsgId());
        long offset = processQueue.removeMessage(Collections.singletonList(messageExt));
        try {
            rocketmqPullConsumer.updateConsumeOffset(messageQueue, offset);
        } catch (MQClientException e) {
            log.error("An error occurred in update consume offset process.", e);
        }
    }

    @Override
    public void startup() {
        this.cleanExpireMsgExecutors.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                cleanExpireMsg();
            }
        }, clientConfig.getRmqMessageConsumeTimeout(), clientConfig.getRmqMessageConsumeTimeout(), TimeUnit.MINUTES);
    }

    @Override
    public void shutdown() {
        ThreadUtils.shutdownGracefully(cleanExpireMsgExecutors, 5000, TimeUnit.MILLISECONDS);
    }

    private void cleanExpireMsg() {
        for (final Map.Entry<MessageQueue, ProcessQueue> next : rocketmqPullConsumer.getDefaultMQPullConsumerImpl().getRebalanceImpl().getProcessQueueTable().entrySet()) {
            ProcessQueue pq = next.getValue();
            MessageQueue mq = next.getKey();
            ReadWriteLock lockTreeMap = getLockInProcessQueue(pq);
            if (lockTreeMap == null) {
                log.error("Gets tree map lock in process queue error, may be has compatibility issue");
                return;
            }

            TreeMap<Long, MessageExt> msgTreeMap = pq.getMsgTreeMap();

            int loop = msgTreeMap.size();
            for (int i = 0; i < loop; i++) {
                MessageExt msg = null;
                try {
                    lockTreeMap.readLock().lockInterruptibly();
                    try {
                        if (!msgTreeMap.isEmpty()) {
                            msg = msgTreeMap.firstEntry().getValue();
                            if (System.currentTimeMillis() - Long.parseLong(MessageAccessor.getConsumeStartTimeStamp(msg)) > clientConfig.getRmqMessageConsumeTimeout() * 60 * 1000) {
                                //Expired, ack and remove it.
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    } finally {
                        lockTreeMap.readLock().unlock();
                    }
                } catch (InterruptedException e) {
                    log.error("Gets expired message exception", e);
                }

                try {
                    rocketmqPullConsumer.sendMessageBack(msg, 3);
                    log.info("Send expired msg back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}", msg.getTopic(), msg.getMsgId(), msg.getStoreHost(), msg.getQueueId(), msg.getQueueOffset());
                    ack(mq, pq, msg);
                } catch (Exception e) {
                    log.error("Send back expired msg exception", e);
                }
            }
        }
    }

    private ReadWriteLock getLockInProcessQueue(ProcessQueue pq) {
        try {
            return (ReadWriteLock) FieldUtils.readDeclaredField(pq, "lockTreeMap", true);
        } catch (IllegalAccessException e) {
            return null;
        }
    }
}
