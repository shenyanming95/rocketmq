/**
 * $Id: ConsumeType.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.heartbeat;

public enum ConsumeType {

    CONSUME_ACTIVELY("PULL"),

    CONSUME_PASSIVELY("PUSH");

    private String typeCN;

    ConsumeType(String typeCN) {
        this.typeCN = typeCN;
    }

    public String getTypeCN() {
        return typeCN;
    }
}
