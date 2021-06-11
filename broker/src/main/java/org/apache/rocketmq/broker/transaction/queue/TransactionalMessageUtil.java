package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.topic.TopicValidator;

import java.nio.charset.Charset;

public class TransactionalMessageUtil {
    public static final String REMOVETAG = "d";
    public static Charset charset = Charset.forName("utf-8");

    public static String buildOpTopic() {
        return TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC;
    }

    public static String buildHalfTopic() {
        return TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC;
    }

    public static String buildConsumerGroup() {
        return MixAll.CID_SYS_RMQ_TRANS;
    }

}
