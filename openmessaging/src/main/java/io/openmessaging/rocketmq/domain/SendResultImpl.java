package io.openmessaging.rocketmq.domain;

import io.openmessaging.KeyValue;
import io.openmessaging.producer.SendResult;

public class SendResultImpl implements SendResult {
    private String messageId;
    private KeyValue properties;

    public SendResultImpl(final String messageId, final KeyValue properties) {
        this.messageId = messageId;
        this.properties = properties;
    }

    @Override
    public String messageId() {
        return messageId;
    }

    public KeyValue properties() {
        return properties;
    }
}
