package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class ResetOffsetBodyTest {

    @Test
    public void testFromJson() throws Exception {
        ResetOffsetBody rob = new ResetOffsetBody();
        Map<MessageQueue, Long> offsetMap = new HashMap<MessageQueue, Long>();
        MessageQueue queue = new MessageQueue();
        queue.setQueueId(1);
        queue.setBrokerName("brokerName");
        queue.setTopic("topic");
        offsetMap.put(queue, 100L);
        rob.setOffsetTable(offsetMap);
        String json = RemotingSerializable.toJson(rob, true);
        ResetOffsetBody fromJson = RemotingSerializable.fromJson(json, ResetOffsetBody.class);
        assertThat(fromJson.getOffsetTable().get(queue)).isEqualTo(100L);
        assertThat(fromJson.getOffsetTable().size()).isEqualTo(1);
    }
}
