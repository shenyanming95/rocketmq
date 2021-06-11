package org.apache.rocketmq.common.sysflag;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PullSysFlagTest {

    @Test
    public void testLitePullFlag() {
        int flag = PullSysFlag.buildSysFlag(false, false, false, false, true);
        assertThat(PullSysFlag.hasLitePullFlag(flag)).isTrue();
    }

    @Test
    public void testLitePullFlagFalse() {
        int flag = PullSysFlag.buildSysFlag(false, false, false, false, false);
        assertThat(PullSysFlag.hasLitePullFlag(flag)).isFalse();
    }
}
