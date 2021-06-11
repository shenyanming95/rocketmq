package org.apache.rocketmq.client.hook;

import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageType;

import java.util.Map;

public class SendMessageContext {
    private String producerGroup;
    private Message message;
    private MessageQueue mq;
    private String brokerAddr;
    private String bornHost;
    private CommunicationMode communicationMode;
    private SendResult sendResult;
    private Exception exception;
    private Object mqTraceContext;
    private Map<String, String> props;
    private DefaultMQProducerImpl producer;
    private MessageType msgType = MessageType.Normal_Msg;
    private String namespace;

    public MessageType getMsgType() {
        return msgType;
    }

    public void setMsgType(final MessageType msgType) {
        this.msgType = msgType;
    }

    public DefaultMQProducerImpl getProducer() {
        return producer;
    }

    public void setProducer(final DefaultMQProducerImpl producer) {
        this.producer = producer;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public MessageQueue getMq() {
        return mq;
    }

    public void setMq(MessageQueue mq) {
        this.mq = mq;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }

    public CommunicationMode getCommunicationMode() {
        return communicationMode;
    }

    public void setCommunicationMode(CommunicationMode communicationMode) {
        this.communicationMode = communicationMode;
    }

    public SendResult getSendResult() {
        return sendResult;
    }

    public void setSendResult(SendResult sendResult) {
        this.sendResult = sendResult;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public Object getMqTraceContext() {
        return mqTraceContext;
    }

    public void setMqTraceContext(Object mqTraceContext) {
        this.mqTraceContext = mqTraceContext;
    }

    public Map<String, String> getProps() {
        return props;
    }

    public void setProps(Map<String, String> props) {
        this.props = props;
    }

    public String getBornHost() {
        return bornHost;
    }

    public void setBornHost(String bornHost) {
        this.bornHost = bornHost;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
}
