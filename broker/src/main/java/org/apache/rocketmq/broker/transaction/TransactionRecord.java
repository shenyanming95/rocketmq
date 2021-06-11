package org.apache.rocketmq.broker.transaction;

/**
 * This class will be removed in the version 4.4.0 and {@link OperationResult} class is recommended.
 */
@Deprecated
public class TransactionRecord {
    // Commit Log Offset
    private long offset;
    private String producerGroup;

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }
}
