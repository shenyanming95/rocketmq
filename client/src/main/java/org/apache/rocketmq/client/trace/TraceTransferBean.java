package org.apache.rocketmq.client.trace;

import java.util.HashSet;
import java.util.Set;

/**
 * Trace transfering bean
 */
public class TraceTransferBean {
    private String transData;
    private Set<String> transKey = new HashSet<String>();

    public String getTransData() {
        return transData;
    }

    public void setTransData(String transData) {
        this.transData = transData;
    }

    public Set<String> getTransKey() {
        return transKey;
    }

    public void setTransKey(Set<String> transKey) {
        this.transKey = transKey;
    }
}
