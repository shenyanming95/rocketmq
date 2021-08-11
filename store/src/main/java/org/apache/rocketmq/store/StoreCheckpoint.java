package org.apache.rocketmq.store;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

/**
 * 控制 rocketMQ 日志数据存储目录下的 checkpoint 文件.
 */
public class StoreCheckpoint {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * checkpoint 文件对应的随机读写句柄
     */
    private final RandomAccessFile randomAccessFile;

    /**
     * checkpoint 文件对应的文件通道
     */
    private final FileChannel fileChannel;

    /**
     * checkpoint 文件对应的内存映射缓冲区
     */
    private final MappedByteBuffer mappedByteBuffer;

    /**
     * commitlog 文件最后刷盘的时间
     */
    private volatile long physicMsgTimestamp = 0;

    /**
     * consumequeue 文件最终刷盘的时间
     */
    private volatile long logicsMsgTimestamp = 0;

    /**
     * index 文件最终刷盘的时间
     */
    private volatile long indexMsgTimestamp = 0;


    public StoreCheckpoint(final String scpPath) throws IOException {
        // 创建文件句柄
        File file = new File(scpPath);
        MappedFile.ensureDirOK(file.getParent());
        boolean fileExists = file.exists();

        // 创建内存映射文件
        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.fileChannel = this.randomAccessFile.getChannel();
        this.mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, MappedFile.OS_PAGE_SIZE);

        // 如果文件已经存在, 将数据从磁盘中读取出来,
        if (fileExists) {
            log.info("store checkpoint file exists, " + scpPath);
            this.physicMsgTimestamp = this.mappedByteBuffer.getLong(0);
            this.logicsMsgTimestamp = this.mappedByteBuffer.getLong(8);
            this.indexMsgTimestamp = this.mappedByteBuffer.getLong(16);
            log.info("store checkpoint file physicMsgTimestamp " + this.physicMsgTimestamp + ", " + UtilAll.timeMillisToHumanString(this.physicMsgTimestamp));
            log.info("store checkpoint file logicsMsgTimestamp " + this.logicsMsgTimestamp + ", " + UtilAll.timeMillisToHumanString(this.logicsMsgTimestamp));
            log.info("store checkpoint file indexMsgTimestamp " + this.indexMsgTimestamp + ", " + UtilAll.timeMillisToHumanString(this.indexMsgTimestamp));
        } else {
            log.info("store checkpoint file not exists, " + scpPath);
        }
    }

    public void shutdown() {
        // 刷盘
        this.flush();

        // unmap mappedByteBuffer
        MappedFile.clean(this.mappedByteBuffer);

        try {
            // 关闭文件通道
            this.fileChannel.close();
        } catch (IOException e) {
            log.error("Failed to properly close the channel", e);
        }
    }

    /**
     * 刷盘, 将三个时间戳依次存储到磁盘中
     */
    public void flush() {
        this.mappedByteBuffer.putLong(0, this.physicMsgTimestamp);
        this.mappedByteBuffer.putLong(8, this.logicsMsgTimestamp);
        this.mappedByteBuffer.putLong(16, this.indexMsgTimestamp);
        this.mappedByteBuffer.force();
    }

    public long getPhysicMsgTimestamp() {
        return physicMsgTimestamp;
    }

    public void setPhysicMsgTimestamp(long physicMsgTimestamp) {
        this.physicMsgTimestamp = physicMsgTimestamp;
    }

    public long getLogicsMsgTimestamp() {
        return logicsMsgTimestamp;
    }

    public void setLogicsMsgTimestamp(long logicsMsgTimestamp) {
        this.logicsMsgTimestamp = logicsMsgTimestamp;
    }

    public long getMinTimestampIndex() {
        return Math.min(this.getMinTimestamp(), this.indexMsgTimestamp);
    }

    public long getMinTimestamp() {
        long min = Math.min(this.physicMsgTimestamp, this.logicsMsgTimestamp);

        min -= 1000 * 3;
        if (min < 0) min = 0;

        return min;
    }

    public long getIndexMsgTimestamp() {
        return indexMsgTimestamp;
    }

    public void setIndexMsgTimestamp(long indexMsgTimestamp) {
        this.indexMsgTimestamp = indexMsgTimestamp;
    }

}
