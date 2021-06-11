package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CheckClientRequestBodyTest {

    @Test
    public void testFromJson() {
        SubscriptionData subscriptionData = new SubscriptionData();
        String expectedClientId = "defalutId";
        String expectedGroup = "defaultGroup";
        CheckClientRequestBody checkClientRequestBody = new CheckClientRequestBody();
        checkClientRequestBody.setClientId(expectedClientId);
        checkClientRequestBody.setGroup(expectedGroup);
        checkClientRequestBody.setSubscriptionData(subscriptionData);
        String json = RemotingSerializable.toJson(checkClientRequestBody, true);
        CheckClientRequestBody fromJson = RemotingSerializable.fromJson(json, CheckClientRequestBody.class);
        assertThat(fromJson.getClientId()).isEqualTo(expectedClientId);
        assertThat(fromJson.getGroup()).isEqualTo(expectedGroup);
        assertThat(fromJson.getSubscriptionData()).isEqualTo(subscriptionData);
    }
}