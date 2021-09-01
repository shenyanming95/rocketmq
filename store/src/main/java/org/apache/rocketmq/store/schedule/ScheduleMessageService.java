package org.apache.rocketmq.store.schedule;

import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 针对延迟消息的实现.
 * <p>
 * rocketMQ的延迟消息无法自定义延迟精度, 它一般通过{@link MessageStoreConfig#getMessageDelayLevel()}来指定多个级别的延迟时间.
 * 每个延迟级别它都会创建一个新的{@link ConsumeQueue}, 然后一旦客户端配置了延迟消息, 消息就会落入到这些延迟{@link ConsumeQueue}队列中.
 * <p>
 * 这个类存在一个定时调度器{@link ScheduleMessageService#timer}, 它会不断地从这些延迟消费队列中拿出延迟消费信息, 判断它们是否已经可以被推送：
 * - 如果可以, 那么会从commit log中拿出消息, 恢复它原先的消息内容, 重新投递到commit log中, 然后推送给客户端;
 * - 如果不行, 那么会计算还需要延迟多长时间, 再创建一个新的任务, 将其投递会{@link ScheduleMessageService#timer}, 等待下次调度.
 */
public class ScheduleMessageService extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 定时任务第一次启动时的延迟时间
     */
    private static final long FIRST_DELAY_TIME = 1000L;

    /**
     * 一次for循环处理完延迟队列后, 下一次调度的延迟时间
     */
    private static final long DELAY_FOR_A_WHILE = 100L;

    /**
     * 执行定时任务异常时, 会重试调度, 这个值就是来指定下一次调度的时间, 默认10s
     */
    private static final long DELAY_FOR_A_PERIOD = 10000L;

    /**
     * 消息延迟的具体毫秒数, 格式为：<延迟等级, 具体延迟毫秒数>
     * 其中, 固定的延迟等级有18个, 从小到大依次为：1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h.
     * 对应的level 为：1, 2, .... , 17, 18
     */
    private final ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable = new ConcurrentHashMap<Integer, Long>(32);

    /**
     * 消息延迟的偏移量, <延迟等级, consumerQueue偏移量>
     */
    private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable = new ConcurrentHashMap<Integer, Long>(32);

    private final DefaultMessageStore defaultMessageStore;

    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * 定时任务调度器, 会在{@link #start()}初始化
     */
    private Timer timer;

    private MessageStore writeMessageStore;

    /**
     * 最大的延迟等级, 是在解析过程中界定的：{@link ScheduleMessageService#parseDelayLevel()}
     */
    private int maxDelayLevel;

    public ScheduleMessageService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.writeMessageStore = defaultMessageStore;
    }

    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }

    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }

    public void setWriteMessageStore(MessageStore writeMessageStore) {
        this.writeMessageStore = writeMessageStore;
    }

    public void buildRunningStats(HashMap<String, String> stats) {
        Iterator<Map.Entry<Integer, Long>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Long> next = it.next();
            int queueId = delayLevel2QueueId(next.getKey());
            long delayOffset = next.getValue();
            long maxOffset = this.defaultMessageStore.getMaxOffsetInQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }

    private void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
    }

    /**
     * 计算延迟消息, 真正需要推送的时间
     *
     * @param delayLevel     延迟等级
     * @param storeTimestamp 消息存储时间戳
     * @return 时间累加值
     */
    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        // 取延迟等级对应的延迟毫秒数, 加上消息的存储时间点, 则等于真正要推送的时间点.
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }
        // 降级处理：不存在延迟等级, 就在存储时间点1s后推送.
        return storeTimestamp + 1000;
    }

    /**
     * 启动该服务
     */
    public void start() {
        // CAS保证并发启动
        if (started.compareAndSet(false, true)) {

            // 初始化时间调度器
            this.timer = new Timer("ScheduleMessageTimerThread", true);

            // 默认总共有18个延迟等级, 每个等级启动一个定时任务
            for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {

                // 延迟级别、延迟时间数
                Integer level = entry.getKey();
                Long timeDelay = entry.getValue();

                // 根据延迟级别获取consumerQueue偏移量, 一开始基本都从0开始
                Long offset = this.offsetTable.get(level);
                if (null == offset) {
                    offset = 0L;
                }

                // 所有延迟等级第一次启动, 都延迟一秒
                if (timeDelay != null) {
                    this.timer.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME);
                }
            }

            // 再开启一个重复调度的任务, 延迟10秒后开启, 然后每隔10秒调度一次. 主要是将 ScheduleMessageService#offsetTable 变量持久化到磁盘中.
            this.timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    try {
                        // 执行刷盘
                        if (started.get()) ScheduleMessageService.this.persist();
                    } catch (Throwable e) {
                        log.error("scheduleAtFixedRate flush exception", e);
                    }
                }
            }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval());
        }
    }

    public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            if (null != this.timer) this.timer.cancel();
        }
    }

    public boolean isStarted() {
        return started.get();
    }

    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }

    public String encode() {
        return this.encode(false);
    }

    public boolean load() {
        // 调用父类方法加载配置文件：delayOffset.json.
        boolean result = super.load();
        // 加载成功后解析延迟等级配置
        result = result && this.parseDelayLevel();
        return result;
    }

    @Override
    public String configFilePath() {
        //配置文件路径：${ROCKET_HOME}/config/delayOffset.json
        return StorePathConfigHelper.getDelayOffsetStorePath(this.defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
            }
        }
    }

    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }

    /**
     * 解析延迟消息的延迟等级
     *
     * @return true-解析成功
     */
    public boolean parseDelayLevel() {
        // 时间单位的map, 单位是以毫秒来统计的
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        timeUnitTable.put("s", 1000L);                //秒
        timeUnitTable.put("m", 1000L * 60);           //分
        timeUnitTable.put("h", 1000L * 60 * 60);      //时
        timeUnitTable.put("d", 1000L * 60 * 60 * 24); //天

        // 获取配置的延迟消息等级：
        // 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
        String levelString = this.defaultMessageStore.getMessageStoreConfig().getMessageDelayLevel();

        try {
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                // 按空格分割后, 取出每一个值
                String value = levelArray[i];
                // ch表示单位, 从单位表中拿出来具体的毫秒数
                String ch = value.substring(value.length() - 1);
                Long tu = timeUnitTable.get(ch);

                // 延迟等级校验
                int level = i + 1;
                if (level > this.maxDelayLevel) {
                    this.maxDelayLevel = level;
                }

                // 计算出具体的延迟毫秒数, 存入表中
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                long delayTimeMillis = tu * num;
                this.delayLevelTable.put(level, delayTimeMillis);
            }
        } catch (Exception e) {
            log.error("parseDelayLevel exception", e);
            log.info("levelString String = {}", levelString);
            return false;
        }
        return true;
    }

    /**
     * 延迟任务, 这里是延迟消息的主要实现.
     */
    class DeliverDelayedMessageTimerTask extends TimerTask {
        /**
         * 延迟等级, 取值只能为{@link MessageStoreConfig#getMessageDelayLevel()}
         */
        private final int delayLevel;

        /**
         * consumer queue 的偏移量, 所有延迟等级, 一开始的偏移量都为0.
         * 在rocketMQ启动一段时间后, 如果使用过延迟消息, 那么对于的延迟等级队列就有相应的offset被记录到磁盘.
         * 此时的偏移量就是磁盘记录的值
         */
        private final long offset;

        public DeliverDelayedMessageTimerTask(int delayLevel, long offset) {
            this.delayLevel = delayLevel;
            this.offset = offset;
        }

        @Override
        public void run() {
            try {
                if (isStarted()) {
                    // 延迟时间到, 就会调用这个方法
                    this.executeOnTimeup();
                }
            } catch (Exception e) {
                // 出现异常后, 先打印日志, 然后重新将这个任务提交到 Timer 中, 此时的延迟时间为：10s
                log.error("ScheduleMessageService, executeOnTimeup exception", e);
                ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel, this.offset), DELAY_FOR_A_PERIOD);
            }
        }

        /**
         * 计算延迟消息要被推送的时间
         *
         * @param now              当前时间
         * @param deliverTimestamp 预计推送时间
         * @return 实际推送时间
         */
        private long correctDeliverTimestamp(final long now, final long deliverTimestamp) {
            // 默认就以用户设置的时间来推送
            long result = deliverTimestamp;
            // 根据任务配置的延迟等级, 计算出它最大的延迟时间
            long maxTimestamp = now + ScheduleMessageService.this.delayLevelTable.get(this.delayLevel);
            // 如果用户配置的时间大于最大延迟时间, 那么就立即返回当前时间.
            // 这样子调用方拿到这个时间点减去now, 为0, 那么就可以立即推送.
            if (deliverTimestamp > maxTimestamp) {
                result = now;
            }
            return result;
        }

        /**
         * 延迟消息的主要处理逻辑
         */
        public void executeOnTimeup() {
            // rocketMQ对于延迟消息, 自己定义了一个主题名称, 就是：SCHEDULE_TOPIC_XXXX, 所有配置了延迟功能的消息都会先存储到这个主题上.
            // 延迟等级减一得到的值就表示队列号-queueId. 通过这两个值可以定位到 consumer queue, 不同的延迟等级具有不同的消费队列
            ConsumeQueue cq = ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC,
                    delayLevel2QueueId(delayLevel));

            // 用来记录调度失败的偏移量, 用于下次调度
            long failScheduleOffset = offset;

            // 该延迟等级对应的消费队列consumerqueue不存在
            if (cq != null) {
                // 查询偏移量在对应的 consumerqueue 中的数据
                SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(this.offset);
                // bufferCQ存在, 说明存在延迟消息
                if (bufferCQ != null) {
                    try {
                        long nextOffset = offset;
                        int i = 0;
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();

                        // 依次取出当前级别的延迟队列中的每条consumer queue消息.
                        for (; i < bufferCQ.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {

                            // 取出 consumer queue 的存储结构, 依次是：commitlog偏移量、消息大小、消息标签编码(在这里存储的是要推送的时间点)
                            long offsetPy = bufferCQ.getByteBuffer().getLong();
                            int sizePy = bufferCQ.getByteBuffer().getInt();
                            long tagsCode = bufferCQ.getByteBuffer().getLong();

                            // consumerqueue ext
                            if (cq.isExtAddr(tagsCode)) {
                                if (cq.getExt(tagsCode, cqExtUnit)) {
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    //can't find ext content.So re compute tags code.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}", tagsCode, offsetPy, sizePy);
                                    long msgStoreTime = defaultMessageStore.getCommitLog().pickupStoreTimestamp(offsetPy, sizePy);
                                    tagsCode = computeDeliverTimestamp(delayLevel, msgStoreTime);
                                }
                            }

                            // 获取当前时间, 同时根据消息配置, 计算它需要被推送的时间
                            long now = System.currentTimeMillis();
                            long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);

                            // 下一个偏移量, 因为i是以 ConsumeQueue.CQ_STORE_UNIT_SIZE 递增的, 直接除以这个值, 得到它的序号.
                            // 一开始的时候 i=0, 此时 nextOffset 等于 offset 自己. 只有自己这条消息处理成功后, 进入下一次循环,
                            // 这个值才会等于下一条 consumerqueue 条目.
                            nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                            // 计算需要延迟的时间
                            long countdown = deliverTimestamp - now;

                            // 如果不需要延迟了, 那么立即推送
                            if (countdown <= 0) {
                                // 通过 commit log 物理偏移量和消息大小, 定位到消息, 对其进行解码得到 MessageExt
                                MessageExt msgExt = ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(offsetPy, sizePy);
                                // 查找到消息就进行处理
                                if (msgExt != null) {
                                    try {
                                        // 解析延迟消息, 还原出它真实的 topic 和 queueId
                                        MessageExtBrokerInner msgInner = this.messageTimeup(msgExt);
                                        // 校验消息的实际topic
                                        if (TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC.equals(msgInner.getTopic())) {
                                            log.error("[BUG] the real topic of schedule msg is {}, discard the msg. msg={}", msgInner.getTopic(), msgInner);
                                            continue;
                                        }
                                        // 将其再保存到 commit log 中(注意：之前已经存储过一次commit log了, 这边会重新再存储一次)
                                        // DefaultMessageStore 有一个专门的重刷线程：ReputMessageService, 用来解析 commit log 保存到 consumer queue中.
                                        PutMessageResult putMessageResult = ScheduleMessageService.this.writeMessageStore.putMessage(msgInner);

                                        if (putMessageResult != null && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                                            // 消息保存成功, 就处理下一条延迟消息
                                            continue;
                                        } else {
                                            // 消息处理失败, 切换到下一条延迟消息处理
                                            log.error("ScheduleMessageService, a message time up, but reput it failed, topic: {} msgId {}", msgExt.getTopic(), msgExt.getMsgId());
                                            // 这边容易产生疑惑? 为何消息处理失败, 但处理的却是 nextOffset 呢?
                                            // 其实, 这边外部是一个for循环, nextOffset 是通过成员变量offset计算的下一条consumerqueue条目,
                                            // for循环第一次遍历, 由于 i=0, 因此 nextOffset==offset, 此时如果消息处理失败, 重试的还是本条消息;
                                            // for循环第二次遍历, 由于 i=1, 因此 nextOffset=offset+1(逻辑), 此时消息处理失败, 重试的是nextOffset, 还是当前处理失败的消息
                                            // ... 其它循环类似.
                                            ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset), DELAY_FOR_A_PERIOD);
                                            ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                                            // 方法返回, 标志这个定时任务结束, 等待上面新加的定时任务重新调度
                                            return;
                                        }
                                    } catch (Exception e) {
                                        // 异常发生, 就会终止这个级别的延迟消息处理逻辑
                                        log.error("ScheduleMessageService, messageTimeup execute error, drop it. msgExt=" + msgExt + ", nextOffset=" + nextOffset + ",offsetPy=" + offsetPy + ",sizePy=" + sizePy, e);
                                    }
                                }
                            } else { // 还需要继续延迟
                                // 同理, 这边为啥用 nextOffset 的原因和上面代码(407行)的原因是一样的.
                                ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset), countdown);
                                ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                                // 方法返回, 标志这个定时任务结束, 等待上面新加的定时任务重新调度
                                return;
                            }
                        } // end of for

                        // for循环处理完以后, 说明在本次定时任务中, 这个级别的consumer queue所有延迟消息都已经被处理好了.
                        // 此时重新计算下一个位置, 然后交由新的定时任务来完成. 有没有发现, 这个i其实是在最后一次for遍历中
                        // 又被i += ConsumeQueue.CQ_STORE_UNIT_SIZE过的, 此时正好处于下一个consumer queue条目(甚至可能还没有数据)
                        nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset), DELAY_FOR_A_WHILE);
                        ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                        return;
                    } finally {
                        bufferCQ.release();
                    }
                }
                // bufferCQ不存在, 不存在延迟消息
                else {
                    // 如果当前延迟级别不存在延迟消息, 获取这个 consumerqueue 的最小逻辑偏移量
                    long cqMinOffset = cq.getMinOffsetInQueue();
                    // 如果偏移量小于当前consumerqueue的最小逻辑偏移量, 那么用最小逻辑偏移量来进行下一次定时调度
                    if (offset < cqMinOffset) {
                        failScheduleOffset = cqMinOffset;
                        log.error("schedule CQ offset invalid. offset=" + offset + ", cqMinOffset=" + cqMinOffset + ", queueId=" + cq.getQueueId());
                    }
                }
            }

            // 两种情况会进入到下面这行代码：
            // 1. level对应的consumerqueue不存在
            // 2. 当前延迟级别不存在延迟消息
            // 重新将这个级别, failScheduleOffset偏移量的任务丢到timer中, 此时延迟执行的时间变为100ms
            ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel, failScheduleOffset), DELAY_FOR_A_WHILE);
        }

        /**
         * 将延迟消息解析成正常消息
         *
         * @param msgExt 延迟消息
         * @return 原消息
         */
        private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setBody(msgExt.getBody());
            msgInner.setFlag(msgExt.getFlag());
            MessageAccessor.setProperties(msgInner, msgExt.getProperties());

            TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
            long tagsCodeValue = MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
            msgInner.setTagsCode(tagsCodeValue);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

            msgInner.setSysFlag(msgExt.getSysFlag());
            msgInner.setBornTimestamp(msgExt.getBornTimestamp());
            msgInner.setBornHost(msgExt.getBornHost());
            msgInner.setStoreHost(msgExt.getStoreHost());
            msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

            msgInner.setWaitStoreMsgOK(false);
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);

            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

            String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
            int queueId = Integer.parseInt(queueIdStr);
            msgInner.setQueueId(queueId);

            return msgInner;
        }
    }
}
