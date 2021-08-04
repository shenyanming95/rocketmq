package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * 缓冲区{@link ByteBuffer}对象池, 底层用的是双端队列{@link Deque}
 */
public class TransientStorePool {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 对象池大小
     */
    private final int poolSize;

    /**
     * 用来控制创建的{@link ByteBuffer}大小, 单位字节
     */
    private final int fileSize;

    /**
     * 底层对象池
     */
    private final Deque<ByteBuffer> availableBuffers;

    /**
     * 消息存储配置
     */
    private final MessageStoreConfig storeConfig;


    public TransientStorePool(final MessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        this.poolSize = storeConfig.getTransientStorePoolSize();
        this.fileSize = storeConfig.getMappedFileSizeCommitLog();
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }

    /**
     * It's a heavy init method.
     */
    public void init() {
        for (int i = 0; i < poolSize; i++) {
            // 该对象池, 存储的都是直接缓冲区: java.nio.DirectByteBuffer.
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);
            // 获取直接缓冲区的内存地址
            final long address = ((DirectBuffer) byteBuffer).address();
            // 锁住程序的内存地址空间, 防止被 swap
            LibC.INSTANCE.mlock(new Pointer(address), new NativeLong(fileSize));
            // 保存这个直接缓冲区
            availableBuffers.offer(byteBuffer);
        }
    }

    public void destroy() {
        // 就是将每个直接缓冲区的内存地址空间解锁
        for (ByteBuffer byteBuffer : availableBuffers) {
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
    }

    public void returnBuffer(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        byteBuffer.limit(fileSize);
        this.availableBuffers.offerFirst(byteBuffer);
    }

    public ByteBuffer borrowBuffer() {
        ByteBuffer buffer = availableBuffers.pollFirst();
        if (availableBuffers.size() < poolSize * 0.4) {
            log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }

    public int availableBufferNums() {
        if (storeConfig.isTransientStorePoolEnable()) {
            return availableBuffers.size();
        }
        return Integer.MAX_VALUE;
    }
}
