package org.apache.rocketmq.tools.monitor;

import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

public class DefaultMonitorListener implements MonitorListener {
    private final static String LOG_PREFIX = "[MONITOR] ";
    private final static String LOG_NOTIFY = LOG_PREFIX + " [NOTIFY] ";
    private final InternalLogger log = ClientLogger.getLog();

    public DefaultMonitorListener() {
    }

    @Override
    public void beginRound() {
        log.info(LOG_PREFIX + "=========================================beginRound");
    }

    @Override
    public void reportUndoneMsgs(UndoneMsgs undoneMsgs) {
        log.info(String.format(LOG_PREFIX + "reportUndoneMsgs: %s", undoneMsgs));
    }

    @Override
    public void reportFailedMsgs(FailedMsgs failedMsgs) {
        log.info(String.format(LOG_PREFIX + "reportFailedMsgs: %s", failedMsgs));
    }

    @Override
    public void reportDeleteMsgsEvent(DeleteMsgsEvent deleteMsgsEvent) {
        log.info(String.format(LOG_PREFIX + "reportDeleteMsgsEvent: %s", deleteMsgsEvent));
    }

    @Override
    public void reportConsumerRunningInfo(TreeMap<String, ConsumerRunningInfo> criTable) {

        {
            boolean result = ConsumerRunningInfo.analyzeSubscription(criTable);
            if (!result) {
                log.info(String.format(LOG_NOTIFY + "reportConsumerRunningInfo: ConsumerGroup: %s, Subscription different", criTable.firstEntry().getValue().getProperties().getProperty("consumerGroup")));
            }
        }

        {
            Iterator<Entry<String, ConsumerRunningInfo>> it = criTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, ConsumerRunningInfo> next = it.next();
                String result = ConsumerRunningInfo.analyzeProcessQueue(next.getKey(), next.getValue());
                if (!result.isEmpty()) {
                    log.info(String.format(LOG_NOTIFY + "reportConsumerRunningInfo: ConsumerGroup: %s, ClientId: %s, %s", criTable.firstEntry().getValue().getProperties().getProperty("consumerGroup"), next.getKey(), result));
                }
            }
        }
    }

    @Override
    public void endRound() {
        log.info(LOG_PREFIX + "=========================================endRound");
    }
}
