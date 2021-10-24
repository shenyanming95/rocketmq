package org.apache.rocketmq.store;

/**
 * 这个类用来分发 commitlog. 什么叫分发呢?
 * 比如 rocketMQ 重启, 需要从磁盘中重新解析出 commitlog 文件, 此时不仅要解析出{@link MappedFile},
 * 同时还要解析出索引{@link org.apache.rocketmq.store.index.IndexFile}.
 * 所以这个类就是用来给其它组件分发commitlog数据.
 */
public interface CommitLogDispatcher {

    void dispatch(final DispatchRequest request);

}
