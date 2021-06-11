package org.apache.rocketmq.broker.transaction;

import java.util.List;

/**
 * This class will be removed in ther version 4.4.0, and {@link TransactionalMessageService} class is recommended.
 */
@Deprecated
public interface TransactionStore {
    boolean open();

    void close();

    boolean put(final List<TransactionRecord> trs);

    void remove(final List<Long> pks);

    List<TransactionRecord> traverse(final long pk, final int nums);

    long totalRecords();

    long minPK();

    long maxPK();
}
