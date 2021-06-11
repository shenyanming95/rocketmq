package org.apache.rocketmq.common;

import org.apache.rocketmq.common.protocol.body.RegisterBrokerBody;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;

public class RegisterBrokerBodyTest {
    @Test
    public void test_encode_decode() throws IOException {
        RegisterBrokerBody registerBrokerBody = new RegisterBrokerBody();
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        registerBrokerBody.setTopicConfigSerializeWrapper(topicConfigSerializeWrapper);

        ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<String, TopicConfig>();
        for (int i = 0; i < 10000; i++) {
            topicConfigTable.put(String.valueOf(i), new TopicConfig(String.valueOf(i)));
        }

        topicConfigSerializeWrapper.setTopicConfigTable(topicConfigTable);

        byte[] compareEncode = registerBrokerBody.encode(true);
        byte[] encode2 = registerBrokerBody.encode(false);
        System.out.println(compareEncode.length);
        System.out.println(encode2.length);
        RegisterBrokerBody decodeRegisterBrokerBody = RegisterBrokerBody.decode(compareEncode, true);

        assertEquals(registerBrokerBody.getTopicConfigSerializeWrapper().getTopicConfigTable().size(), decodeRegisterBrokerBody.getTopicConfigSerializeWrapper().getTopicConfigTable().size());

    }
}
