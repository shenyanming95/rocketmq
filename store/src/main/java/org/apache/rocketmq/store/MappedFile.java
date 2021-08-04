package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 这个类是对底层磁盘存储文件的内存映射, 主要使用了 Java Nio 技术的{@link MappedByteBuffer}.
 * 这个类的一个实例对应着一个磁盘文件, 这个类会被{@link MappedFileQueue}管理, 主要用在 RocketMQ 这几个日志数据：
 * commitLog、consumerQueue、indexFile
 */
public class MappedFile extends ReferenceResource {

    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /* static fields */

    /**
     * 操作系统 page cache 的大小, 默认是4KB
     */
    public static final int OS_PAGE_SIZE = 1024 * 4;

    /**
     * 用来统计总的内存映射文件的字节数, 比如映射一个 commit log 文件, 由于它默认是 1G 大小,
     * 所以这个值就会加上 1 * 1024 * 1024 * 1024.
     */
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    /**
     * 同上一个参数一样, 用于统计总映射的文件数量.
     */
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

    /* fields */

    /**
     * 底层磁盘文件的大小：
     * 1.如果是 commit log, 每个文件默认1G;
     * 2.如果是 consume queue, 每个文件默认6M;
     */
    protected int fileSize;

    /**
     * 磁盘文件的绝对路径
     */
    private String fileName;

    /**
     * 文件句柄, 即 file = new File(fileName)
     */
    private File file;

    /**
     * rocketMQ 会把磁盘文件使用虚拟内存的方式直接映射到用户空间上, 其读写性能极高.
     */
    private MappedByteBuffer mappedByteBuffer;

    /**
     * 磁盘文件对应的 nio 文件通道
     */
    protected FileChannel fileChannel;

    /**
     * 磁盘文件的起始偏移量, 大部分情况都是它的文件名.
     * 因为 RocketMQ 在对其维护的日志数据文件取名是很有讲究的, 比如commit log的文件命名,
     * 00000000000000000000 是第一个 commit log 文件, 那么这个文件的起始偏移量就为0;
     * 00000000001073741824 是第二个 commit log 文件, 那么它的起始偏移量为1073741824;
     */
    private long fileFromOffset;

    /**
     * 这两个参数是搭配使用的, 通过{@link TransientStorePool}来获取{@link ByteBuffer}.
     * 对象池{@link TransientStorePool} 维护了许多直接缓冲区(rocketMQ通过JNA执行OS系统调用, 做了优化).
     * 当用户开启了对象池配置, 这两个参数就会生效, 那么数据就会先被保存到这里, 然后在重刷到{@link FileChannel}中.
     */
    protected ByteBuffer writeBuffer = null;
    protected TransientStorePool transientStorePool = null;

    /**
     * 记录消息保存或者刷盘的时间
     */
    private volatile long storeTimestamp = 0;

    /**
     * 如果是在{@link MappedFileQueue}创建的, 这个值就为true.
     */
    private boolean firstCreateInQueue = false;

    /**
     * 当前文件可写的起始位置
     */
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);

    /**
     * 当前文件已提交的最终位置(还未刷盘)
     */
    protected final AtomicInteger committedPosition = new AtomicInteger(0);

    /**
     * 当前文件已刷盘的最终位置
     */
    private final AtomicInteger flushedPosition = new AtomicInteger(0);

    /*
     * Constructor
     */

    /**
     * 创建一个内存映射文件
     *
     * @param fileName 磁盘文件路径
     * @param fileSize 磁盘文件大小
     * @throws IOException IOE
     */
    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    /**
     * 创建一个内存映射文件, 并使用{@link ByteBuffer}对象池
     *
     * @param fileName           磁盘文件路径
     * @param fileSize           磁盘文件大小
     * @param transientStorePool 对象池
     * @throws IOException IOEø
     */
    public MappedFile(final String fileName, final int fileSize, final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    /**
     * 保证该路径表示一个文件目录
     *
     * @param dirName 路径
     */
    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            // 不存在时创建目录
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    /**
     * 释放资源
     *
     * @param buffer nio缓冲区
     */
    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0) {
            return;
        }
        ByteBuffer viewed = viewed(buffer);
        Object invoke = invoke(viewed, "cleaner");
        invoke(invoke, "clean");
    }


    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();

        // 遍历 ByteBuffer 中的每个方法, 取到第一个方法名带有"attachment"的方法.
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        // 代码走到这边, 两种情况：要么等于“viewedBuffer”, 要么等于“attachment”.
        // 所以这边会执行 viewedBuffer()方法, 或者 attachment().
        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);

        // 递归获取
        return viewedBuffer == null ? buffer : viewed(viewedBuffer);
    }

    /**
     * 找到指定方法名的方法, 并回调它
     *
     * @param target     对象实体
     * @param methodName 方法名
     * @param args       方法参数
     * @return 方法执行结果
     */
    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    // 获取指定 methodName 的方法, 设置它的访问级别
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    // 回调方法
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    /**
     * 获取 target 指定方法名的方法
     *
     * @param target     对象实体
     * @param methodName 方法名
     * @param args       参数
     * @return Method
     */
    private static Method method(Object target, String methodName, Class<?>[] args) throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    /*
     * 成员方法
     */

    public void init(final String fileName, final int fileSize, final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        // fileName其实是filePath, 所以可以直接拿来创建一个file句柄
        this.file = new File(fileName);
        // rocketMQ磁盘文件命名是很有规律的, 它的文件名就是起始偏移量
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;
        // 保证该磁盘文件的父目录存在(若不存在, 这个方法会创建它)
        ensureDirOK(this.file.getParent());

        try {
            // 获取该磁盘文件对应的 nio 通道.
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            // 内存映射, 直接将文件映射到用户空间中.
            // 其中参数 0 表示起始映射偏移量, fileSize表示总的要映射的空间
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);

            // 一个用来记录总的映射字节数, 另一个用来记录总的映射文件数
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            // ok 置为true, 表示映射成功, 到这里表示 MapperFile 创建并初始化成功
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    /**
     * 获取底层磁盘文件最后一次更新时间
     *
     * @return 时间戳
     */
    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    /**
     * 获取磁盘文件大小
     *
     * @return 文件大小, 单位字节
     */
    public int getFileSize() {
        return fileSize;
    }

    /**
     * 获取 nio 文件通道
     *
     * @return channel
     */
    public FileChannel getFileChannel() {
        return fileChannel;
    }

    /**
     * 获取底层磁盘文件的起始偏移量
     *
     * @return 偏移量
     */
    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    /**
     * 追加单条消息
     *
     * @param msg 单条消息
     * @param cb  回调方法
     * @return 结果
     */
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        return appendMessagesInner(msg, cb);
    }

    /**
     * 追加批量消息
     *
     * @param messageExtBatch 多条消息
     * @param cb              回调方法
     * @return 结果
     */
    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {
        return appendMessagesInner(messageExtBatch, cb);
    }

    /**
     * 存储消息到 commit log中
     *
     * @param messageExt 消息
     * @param cb         回调方法
     * @return 存储结果
     */
    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {
        assert messageExt != null;
        assert cb != null;

        // 文件可写的最小偏移量
        int currentPos = this.wrotePosition.get();

        // 只有可写偏移量小于文件大小, 消息才可以被追加
        if (currentPos < this.fileSize) {
            // slice()方法, 用于创建一个新的字节缓冲区, 其内容是给定缓冲区内容的共享子序列
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);

            AppendMessageResult result;
            // 强转不同的消息, 最终还是交予 AppendMessageCallback 执行.
            if (messageExt instanceof MessageExtBrokerInner) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt);
            } else if (messageExt instanceof MessageExtBatch) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            // 写入成功后, 更新起始写位置量
            this.wrotePosition.addAndGet(result.getWroteBytes());
            // 更新储存时间
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    /**
     * 写入消息
     * @param data 消息内容
     * @return true-写入成功
     */
    public boolean appendMessage(final byte[] data) {
        // 当前可以开始写的位置量
        int currentPos = this.wrotePosition.get();
        // 不超过文件大小才可以写入
        if ((currentPos + data.length) <= this.fileSize) {
            try {
                // fileChannel 为磁盘 commit log 的操作通道, 先设置它的可写位置值, 然后写入
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            // 写入成功后, 更新起始写位置量
            this.wrotePosition.addAndGet(data.length);
            return true;
        }
        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();
        if ((currentPos + length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }
        return false;
    }

    /**
     * @return The current flushed position
     */
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();

                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    public int commit(final int commitLeastPages) {
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                commit0(commitLeastPages);
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return this.committedPosition.get();
    }

    protected void commit0(final int commitLeastPages) {
        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - lastCommittedPosition > commitLeastPages) {
            try {
                ByteBuffer byteBuffer = writeBuffer.slice();
                byteBuffer.position(lastCommittedPosition);
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.flushedPosition.get();
        int write = getReadPosition();

        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    protected boolean isAbleToCommit(final int commitLeastPages) {
        int flush = this.committedPosition.get();
        int write = this.wrotePosition.get();

        if (this.isFull()) {
            return true;
        }

        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > flush;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: " + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName + " have cleanup, do not do it again.");
            return true;
        }

        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:" + this.getFlushedPosition() + ", " + UtilAll.computeElapsedTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}", this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(), System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
