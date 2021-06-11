package org.apache.rocketmq.common;

import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RemotingUtilTest {
    @Test
    public void testGetLocalAddress() throws Exception {
        String localAddress = RemotingUtil.getLocalAddress();
        assertThat(localAddress).isNotNull();
        assertThat(localAddress.length()).isGreaterThan(0);
    }
}
