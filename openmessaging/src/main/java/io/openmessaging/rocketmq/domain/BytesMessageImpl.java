package io.openmessaging.rocketmq.domain;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.OMS;
import io.openmessaging.exception.OMSMessageFormatException;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class BytesMessageImpl implements BytesMessage {
    private KeyValue sysHeaders;
    private KeyValue userHeaders;
    private byte[] body;

    public BytesMessageImpl() {
        this.sysHeaders = OMS.newKeyValue();
        this.userHeaders = OMS.newKeyValue();
    }

    @Override
    public <T> T getBody(Class<T> type) throws OMSMessageFormatException {
        if (type == byte[].class) {
            return (T) body;
        }

        throw new OMSMessageFormatException("", "Cannot assign byte[] to " + type.getName());
    }

    @Override
    public BytesMessage setBody(final byte[] body) {
        this.body = body;
        return this;
    }

    @Override
    public KeyValue sysHeaders() {
        return sysHeaders;
    }

    @Override
    public KeyValue userHeaders() {
        return userHeaders;
    }

    @Override
    public Message putSysHeaders(String key, int value) {
        sysHeaders.put(key, value);
        return this;
    }

    @Override
    public Message putSysHeaders(String key, long value) {
        sysHeaders.put(key, value);
        return this;
    }

    @Override
    public Message putSysHeaders(String key, double value) {
        sysHeaders.put(key, value);
        return this;
    }

    @Override
    public Message putSysHeaders(String key, String value) {
        sysHeaders.put(key, value);
        return this;
    }

    @Override
    public Message putUserHeaders(String key, int value) {
        userHeaders.put(key, value);
        return this;
    }

    @Override
    public Message putUserHeaders(String key, long value) {
        userHeaders.put(key, value);
        return this;
    }

    @Override
    public Message putUserHeaders(String key, double value) {
        userHeaders.put(key, value);
        return this;
    }

    @Override
    public Message putUserHeaders(String key, String value) {
        userHeaders.put(key, value);
        return this;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
