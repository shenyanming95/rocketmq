package org.apache.rocketmq.common;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MQVersionTest {

    @Test
    public void testGetVersionDesc() throws Exception {
        String desc = "V3_0_0_SNAPSHOT";
        assertThat(MQVersion.getVersionDesc(0)).isEqualTo(desc);
    }

    @Test
    public void testGetVersionDesc_higherVersion() throws Exception {
        String desc = "HIGHER_VERSION";
        assertThat(MQVersion.getVersionDesc(Integer.MAX_VALUE)).isEqualTo(desc);
    }

    @Test
    public void testValue2Version() throws Exception {
        assertThat(MQVersion.value2Version(0)).isEqualTo(MQVersion.Version.V3_0_0_SNAPSHOT);
    }

    @Test
    public void testValue2Version_HigherVersion() throws Exception {
        assertThat(MQVersion.value2Version(Integer.MAX_VALUE)).isEqualTo(MQVersion.Version.HIGHER_VERSION);
    }
}