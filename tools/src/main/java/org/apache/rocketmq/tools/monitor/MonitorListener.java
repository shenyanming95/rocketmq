package org.apache.rocketmq.tools.monitor;

import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;

import java.util.TreeMap;

public interface MonitorListener {
    void beginRound();

    void reportUndoneMsgs(UndoneMsgs undoneMsgs);

    void reportFailedMsgs(FailedMsgs failedMsgs);

    void reportDeleteMsgsEvent(DeleteMsgsEvent deleteMsgsEvent);

    void reportConsumerRunningInfo(TreeMap<String/* clientId */, ConsumerRunningInfo> criTable);

    void endRound();
}
