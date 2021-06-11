package org.apache.rocketmq.broker.mqtrace;

public interface SendMessageHook {
    public String hookName();

    public void sendMessageBefore(final SendMessageContext context);

    public void sendMessageAfter(final SendMessageContext context);
}
