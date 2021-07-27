package org.apache.rocketmq.common.protocol.topic;

import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class OffsetMovedEventTest {

    @Test
    public void testFromJson() throws Exception {
        OffsetMovedEvent event = mockOffsetMovedEvent();

        String json = event.toJson();
        OffsetMovedEvent fromJson = RemotingSerializable.fromJson(json, OffsetMovedEvent.class);

        assertEquals(event, fromJson);
    }

    @Test
    public void testFromBytes() throws Exception {
        OffsetMovedEvent event = mockOffsetMovedEvent();

        byte[] encodeData = event.encode();
        OffsetMovedEvent decodeData = RemotingSerializable.decode(encodeData, OffsetMovedEvent.class);

        assertEquals(event, decodeData);
    }

    private void assertEquals(OffsetMovedEvent srcData, OffsetMovedEvent decodeData) {
        assertThat(decodeData.getConsumerGroup()).isEqualTo(srcData.getConsumerGroup());
        assertThat(decodeData.getMessageQueue().getTopic()).isEqualTo(srcData.getMessageQueue().getTopic());
        assertThat(decodeData.getMessageQueue().getBrokerName()).isEqualTo(srcData.getMessageQueue().getBrokerName());
        assertThat(decodeData.getMessageQueue().getQueueId()).isEqualTo(srcData.getMessageQueue().getQueueId());
        assertThat(decodeData.getOffsetRequest()).isEqualTo(srcData.getOffsetRequest());
        assertThat(decodeData.getOffsetNew()).isEqualTo(srcData.getOffsetNew());
    }

    private OffsetMovedEvent mockOffsetMovedEvent() {
        OffsetMovedEvent event = new OffsetMovedEvent();
        event.setConsumerGroup("test-group");
        event.setMessageQueue(new MessageQueue("test-topic", "test-broker", 0));
        event.setOffsetRequest(3000L);
        event.setOffsetNew(1000L);
        return event;
    }
}
