package org.apache.rocketmq.common.topic;

import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TopicValidatorTest {

    private static void clearResponse(RemotingCommand response) {
        response.setCode(-1);
        response.setRemark("");
    }

    private static String generateString(int length) {
        StringBuilder stringBuffer = new StringBuilder();
        String tmpStr = "0123456789";
        for (int i = 0; i < length; i++) {
            stringBuffer.append(tmpStr);
        }
        return stringBuffer.toString();
    }

    @Test
    public void testTopicValidator_NotPass() {
        RemotingCommand response = RemotingCommand.createResponseCommand(-1, "");

        Boolean res = TopicValidator.validateTopic("", response);
        assertThat(res).isFalse();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        assertThat(response.getRemark()).contains("The specified topic is blank");

        clearResponse(response);
        res = TopicValidator.validateTopic("../TopicTest", response);
        assertThat(res).isFalse();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        assertThat(response.getRemark()).contains("The specified topic contains illegal characters");

        clearResponse(response);
        res = TopicValidator.validateTopic(generateString(128), response);
        assertThat(res).isFalse();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        assertThat(response.getRemark()).contains("The specified topic is longer than topic max length.");
    }

    @Test
    public void testTopicValidator_Pass() {
        RemotingCommand response = RemotingCommand.createResponseCommand(-1, "");

        Boolean res = TopicValidator.validateTopic("TestTopic", response);
        assertThat(res).isTrue();
        assertThat(response.getCode()).isEqualTo(-1);
        assertThat(response.getRemark()).isEmpty();
    }

    @Test
    public void testAddSystemTopic() {
        String topic = "SYSTEM_TOPIC_TEST";
        TopicValidator.addSystemTopic(topic);
        assertThat(TopicValidator.getSystemTopicSet()).contains(topic);
    }

    @Test
    public void testIsSystemTopic() {
        boolean res;
        for (String topic : TopicValidator.getSystemTopicSet()) {
            res = TopicValidator.isSystemTopic(topic);
            assertThat(res).isTrue();
        }

        String topic = TopicValidator.SYSTEM_TOPIC_PREFIX + "_test";
        res = TopicValidator.isSystemTopic(topic);
        assertThat(res).isTrue();

        topic = "test_not_system_topic";
        res = TopicValidator.isSystemTopic(topic);
        assertThat(res).isFalse();
    }

    @Test
    public void testIsSystemTopicWithResponse() {
        RemotingCommand response = RemotingCommand.createResponseCommand(-1, "");
        boolean res;
        for (String topic : TopicValidator.getSystemTopicSet()) {
            res = TopicValidator.isSystemTopic(topic, response);
            assertThat(res).isTrue();
            assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
            assertThat(response.getRemark()).isEqualTo("The topic[" + topic + "] is conflict with system topic.");
        }

        String topic = "test_not_system_topic";
        res = TopicValidator.isSystemTopic(topic, response);
        assertThat(res).isFalse();
    }

    @Test
    public void testIsNotAllowedSendTopic() {
        boolean res;
        for (String topic : TopicValidator.getNotAllowedSendTopicSet()) {
            res = TopicValidator.isNotAllowedSendTopic(topic);
            assertThat(res).isTrue();
        }

        String topic = "test_allowed_send_topic";
        res = TopicValidator.isNotAllowedSendTopic(topic);
        assertThat(res).isFalse();
    }

    @Test
    public void testIsNotAllowedSendTopicWithResponse() {
        RemotingCommand response = RemotingCommand.createResponseCommand(-1, "");

        boolean res;
        for (String topic : TopicValidator.getNotAllowedSendTopicSet()) {
            res = TopicValidator.isNotAllowedSendTopic(topic, response);
            assertThat(res).isTrue();
            assertThat(response.getCode()).isEqualTo(ResponseCode.NO_PERMISSION);
            assertThat(response.getRemark()).isEqualTo("Sending message to topic[" + topic + "] is forbidden.");
        }

        String topic = "test_allowed_send_topic";
        res = TopicValidator.isNotAllowedSendTopic(topic, response);
        assertThat(res).isFalse();
    }
}
