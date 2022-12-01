package org.apache.rocketmq.common.protocol;

/**
 * RocketMQ每个请求都会定义一个Code.
 * 用来标识当前请求的操作
 */
public class RequestCode {


    public static final int SEND_MESSAGE = 10;

    /**
     * Consumer向Broker拉取消息
     */
    public static final int PULL_MESSAGE = 11;

    public static final int QUERY_MESSAGE = 12;
    public static final int QUERY_BROKER_OFFSET = 13;
    public static final int QUERY_CONSUMER_OFFSET = 14;
    public static final int UPDATE_CONSUMER_OFFSET = 15;

    /**
     * 更新或者创建topic
     */
    public static final int UPDATE_AND_CREATE_TOPIC = 17;

    /**
     * 获取所有topic的配置
     */
    public static final int GET_ALL_TOPIC_CONFIG = 21;

    public static final int GET_TOPIC_CONFIG_LIST = 22;

    public static final int GET_TOPIC_NAME_LIST = 23;

    /**
     * 更新broker配置
     */
    public static final int UPDATE_BROKER_CONFIG = 25;

    /**
     * 获取broker配置
     */
    public static final int GET_BROKER_CONFIG = 26;

    public static final int TRIGGER_DELETE_FILES = 27;

    /**
     * 获取broker的实时运行数据
     */
    public static final int GET_BROKER_RUNTIME_INFO = 28;

    /**
     * 根据存储时间戳查询消息的物理偏移量
     */
    public static final int SEARCH_OFFSET_BY_TIMESTAMP = 29;

    /**
     * 获取指定topic, 指定queueId下的最大的commitlog偏移量
     */
    public static final int GET_MAX_OFFSET = 30;

    /**
     * 获取指定topic, 指定queueId下的最小的commitlog偏移量
     */
    public static final int GET_MIN_OFFSET = 31;

    /**
     * 获取指定topic + 指定queueId下, 消息的最早存储时间
     */
    public static final int GET_EARLIEST_MSG_STORETIME = 32;

    public static final int VIEW_MESSAGE_BY_ID = 33;

    public static final int HEART_BEAT = 34;

    public static final int UNREGISTER_CLIENT = 35;

    public static final int CONSUMER_SEND_MSG_BACK = 36;

    /**
     * 提交or回滚事务的请求码
     */
    public static final int END_TRANSACTION = 37;

    public static final int GET_CONSUMER_LIST_BY_GROUP = 38;

    public static final int CHECK_TRANSACTION_STATE = 39;

    public static final int NOTIFY_CONSUMER_IDS_CHANGED = 40;

    /**
     * 重平衡的时候, 锁住指定queue
     */
    public static final int LOCK_BATCH_MQ = 41;

    /**
     * 重平衡的时候, 解锁指定queue
     */
    public static final int UNLOCK_BATCH_MQ = 42;

    /**
     * 获取所有的consumer消费的偏移量
     */
    public static final int GET_ALL_CONSUMER_OFFSET = 43;

    /**
     * 获取所有的延迟消息的偏移量
     */
    public static final int GET_ALL_DELAY_OFFSET = 45;

    public static final int CHECK_CLIENT_CONFIG = 46;

    public static final int UPDATE_AND_CREATE_ACL_CONFIG = 50;

    public static final int DELETE_ACL_CONFIG = 51;

    public static final int GET_BROKER_CLUSTER_ACL_INFO = 52;

    public static final int UPDATE_GLOBAL_WHITE_ADDRS_CONFIG = 53;

    public static final int GET_BROKER_CLUSTER_ACL_CONFIG = 54;

    public static final int PUT_KV_CONFIG = 100;

    public static final int GET_KV_CONFIG = 101;

    public static final int DELETE_KV_CONFIG = 102;

    public static final int REGISTER_BROKER = 103;

    public static final int UNREGISTER_BROKER = 104;
    public static final int GET_ROUTEINFO_BY_TOPIC = 105;

    public static final int GET_BROKER_CLUSTER_INFO = 106;

    /**
     * 更新或者创建订阅组信息
     */
    public static final int UPDATE_AND_CREATE_SUBSCRIPTIONGROUP = 200;

    /**
     * 获取所有订阅组信息
     */
    public static final int GET_ALL_SUBSCRIPTIONGROUP_CONFIG = 201;

    /**
     * 获取topic的汇总指标信息, 比如：最小偏移量、最大偏移量、最后一次操作消息的时间
     */
    public static final int GET_TOPIC_STATS_INFO = 202;

    /**
     * 获取consumer的连接信息列表
     */
    public static final int GET_CONSUMER_CONNECTION_LIST = 203;

    /**
     * 获取producer的连接信息列表
     */
    public static final int GET_PRODUCER_CONNECTION_LIST = 204;

    public static final int WIPE_WRITE_PERM_OF_BROKER = 205;

    public static final int GET_ALL_TOPIC_LIST_FROM_NAMESERVER = 206;

    /**
     * 删除订阅组
     */
    public static final int DELETE_SUBSCRIPTIONGROUP = 207;

    /**
     * 获取 topic-queue 维度下的消息消费汇总指标
     */
    public static final int GET_CONSUME_STATS = 208;

    public static final int SUSPEND_CONSUMER = 209;

    public static final int RESUME_CONSUMER = 210;
    public static final int RESET_CONSUMER_OFFSET_IN_CONSUMER = 211;
    public static final int RESET_CONSUMER_OFFSET_IN_BROKER = 212;

    public static final int ADJUST_CONSUMER_THREAD_POOL = 213;

    public static final int WHO_CONSUME_THE_MESSAGE = 214;

    /**
     * 删除主topic
     */
    public static final int DELETE_TOPIC_IN_BROKER = 215;

    public static final int DELETE_TOPIC_IN_NAMESRV = 216;
    public static final int GET_KVLIST_BY_NAMESPACE = 219;

    public static final int RESET_CONSUMER_CLIENT_OFFSET = 220;

    public static final int GET_CONSUMER_STATUS_FROM_CLIENT = 221;

    public static final int INVOKE_BROKER_TO_RESET_OFFSET = 222;

    public static final int INVOKE_BROKER_TO_GET_CONSUMER_STATUS = 223;

    public static final int QUERY_TOPIC_CONSUME_BY_WHO = 300;

    public static final int GET_TOPICS_BY_CLUSTER = 224;

    public static final int REGISTER_FILTER_SERVER = 301;
    public static final int REGISTER_MESSAGE_FILTER_CLASS = 302;

    public static final int QUERY_CONSUME_TIME_SPAN = 303;

    public static final int GET_SYSTEM_TOPIC_LIST_FROM_NS = 304;
    public static final int GET_SYSTEM_TOPIC_LIST_FROM_BROKER = 305;

    public static final int CLEAN_EXPIRED_CONSUMEQUEUE = 306;

    public static final int GET_CONSUMER_RUNNING_INFO = 307;

    public static final int QUERY_CORRECTION_OFFSET = 308;
    public static final int CONSUME_MESSAGE_DIRECTLY = 309;

    public static final int SEND_MESSAGE_V2 = 310;

    public static final int GET_UNIT_TOPIC_LIST = 311;

    public static final int GET_HAS_UNIT_SUB_TOPIC_LIST = 312;

    public static final int GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST = 313;

    public static final int CLONE_GROUP_OFFSET = 314;

    public static final int VIEW_BROKER_STATS_DATA = 315;

    public static final int CLEAN_UNUSED_TOPIC = 316;

    public static final int GET_BROKER_CONSUME_STATS = 317;

    /**
     * update the config of name server
     */
    public static final int UPDATE_NAMESRV_CONFIG = 318;

    /**
     * get config from name server
     */
    public static final int GET_NAMESRV_CONFIG = 319;

    public static final int SEND_BATCH_MESSAGE = 320;

    public static final int QUERY_CONSUME_QUEUE = 321;

    public static final int QUERY_DATA_VERSION = 322;

    /**
     * resume logic of checking half messages that have been put in TRANS_CHECK_MAXTIME_TOPIC before
     */
    public static final int RESUME_CHECK_HALF_MESSAGE = 323;

    public static final int SEND_REPLY_MESSAGE = 324;

    public static final int SEND_REPLY_MESSAGE_V2 = 325;

    public static final int PUSH_REPLY_MESSAGE_TO_CLIENT = 326;
}
