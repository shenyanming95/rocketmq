package org.apache.rocketmq.client.utils;

import org.apache.rocketmq.client.common.ClientErrorCode;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;

public class MessageUtil {
    public static Message createReplyMessage(final Message requestMessage, final byte[] body) throws MQClientException {
        if (requestMessage != null) {
            Message replyMessage = new Message();
            String cluster = requestMessage.getProperty(MessageConst.PROPERTY_CLUSTER);
            String replyTo = requestMessage.getProperty(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT);
            String correlationId = requestMessage.getProperty(MessageConst.PROPERTY_CORRELATION_ID);
            String ttl = requestMessage.getProperty(MessageConst.PROPERTY_MESSAGE_TTL);
            replyMessage.setBody(body);
            if (cluster != null) {
                String replyTopic = MixAll.getReplyTopic(cluster);
                replyMessage.setTopic(replyTopic);
                MessageAccessor.putProperty(replyMessage, MessageConst.PROPERTY_MESSAGE_TYPE, MixAll.REPLY_MESSAGE_FLAG);
                MessageAccessor.putProperty(replyMessage, MessageConst.PROPERTY_CORRELATION_ID, correlationId);
                MessageAccessor.putProperty(replyMessage, MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, replyTo);
                MessageAccessor.putProperty(replyMessage, MessageConst.PROPERTY_MESSAGE_TTL, ttl);

                return replyMessage;
            } else {
                throw new MQClientException(ClientErrorCode.CREATE_REPLY_MESSAGE_EXCEPTION, "create reply message fail, requestMessage error, property[" + MessageConst.PROPERTY_CLUSTER + "] is null.");
            }
        }
        throw new MQClientException(ClientErrorCode.CREATE_REPLY_MESSAGE_EXCEPTION, "create reply message fail, requestMessage cannot be null.");
    }

    public static String getReplyToClient(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT);
    }
}
