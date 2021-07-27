package org.apache.rocketmq.client.exception;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.help.FAQUrl;

public class MQBrokerException extends Exception {
    private static final long serialVersionUID = 5975020272601250368L;
    private final int responseCode;
    private final String errorMessage;
    private final String brokerAddr;

    public MQBrokerException(int responseCode, String errorMessage) {
        super(FAQUrl.attachDefaultURL("CODE: " + UtilAll.responseCode2String(responseCode) + "  DESC: " + errorMessage));
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
        this.brokerAddr = null;
    }

    public MQBrokerException(int responseCode, String errorMessage, String brokerAddr) {
        super(FAQUrl.attachDefaultURL("CODE: " + UtilAll.responseCode2String(responseCode) + "  DESC: " + errorMessage + (brokerAddr != null ? " BROKER: " + brokerAddr : "")));
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
        this.brokerAddr = brokerAddr;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }
}
