package org.apache.rocketmq.common.protocol;

import org.apache.rocketmq.common.protocol.body.ConsumeStatus;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

public class ConsumeStatusTest {

    @Test
    public void testFromJson() throws Exception {
        ConsumeStatus cs = new ConsumeStatus();
        cs.setConsumeFailedTPS(10);
        cs.setPullRT(100);
        cs.setPullTPS(1000);
        String json = RemotingSerializable.toJson(cs, true);
        ConsumeStatus fromJson = RemotingSerializable.fromJson(json, ConsumeStatus.class);
        assertThat(fromJson.getPullRT()).isCloseTo(cs.getPullRT(), within(0.0001));
        assertThat(fromJson.getPullTPS()).isCloseTo(cs.getPullTPS(), within(0.0001));
        assertThat(fromJson.getConsumeFailedTPS()).isCloseTo(cs.getConsumeFailedTPS(), within(0.0001));
    }

}
