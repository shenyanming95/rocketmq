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
import org.apache.rocketmq.store.config.MessageStoreConfig;
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
     * 注：数据写到这里会被刷新到磁盘.
     */
    private MappedByteBuffer mappedByteBuffer;

    /**
     * 磁盘文件对应的 nio 文件通道.
     * 注：数据写到这里会被刷新到磁盘.
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
     * 当用户开启了对象池配置, 这两个参数就会生效, 那么数据就会先被保存到这里(称为commit, 即调用了{@link #commit(int)}),
     * 然后在重刷到{@link FileChannel}中.
     * <p>
     * 注：数据写到这里不会被刷新到磁盘, 所以这个 writeBuffer 只是一个暂存区, 它需要将数据转存到 fileChannel 或者 mappedByteBuffer.
     */
    protected ByteBuffer writeBuffer = null;
    protected TransientStorePool transientStorePool = null;

    /**
     * 记录消息保存或者刷盘的时间
     */
    private volatile long storeTimestamp = 0;

    /**
     * 如果是在{@link MappedFileQueue}创建的, 并且是第一个创建的, 这个值就为true.
     */
    private boolean firstCreateInQueue = false;

    /**
     * 当前文件已写入的位置, 要么写入到{@link #mappedByteBuffer}, 要么写入到{@link #writeBuffer}
     */
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);

    /**
     * 当前文件已提交的最终位置, 此时还未刷盘. 这个参数只有在{@link #writeBuffer}不为空的情况下才有效,
     * 也就是说 MappedFile 只有在开启{@link MessageStoreConfig#isTransientStorePoolEnable()}才会有 commit 的概念.
     * (被提交的消息会被存储到{@link #writeBuffer})
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
     * 存储消息
     *
     * @param msg 单条消息
     * @param cb  回调方法
     * @return 结果
     */
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        return appendMessagesInner(msg, cb);
    }

    /**
     * 存储消息
     *
     * @param messageExtBatch 多条消息
     * @param cb              回调方法
     * @return 结果
     */
    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {
        return appendMessagesInner(messageExtBatch, cb);
    }

    /**
     * 存储消息
     *
     * @param messageExt 消息
     * @param cb         回调方法
     * @return 存储结果
     */
    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {
        assert messageExt != null;
        assert cb != null;

        // 当前文件可写的起始偏移量
        int currentPos = this.wrotePosition.get();

        // 只有起始偏移量小于文件大小, 消息才可以被存储
        if (currentPos < this.fileSize) {
            // slice()方法, 用于创建一个新的缓冲区, 它的原先的缓冲区共用一个底层数组, 但是它却无法读取到原先缓冲区position之前的数据(因为内部会指定一个offset).
            // 但是它们的一些指针比如：position、limit、capacity...是独立的, 互不影响.
            // 而且, slice() 出来的缓冲区, position=0, limit=capacity=原先缓冲区的limit-position.
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            // 使用 slice() 方法创建出来的 ByteBuffer, 如果不手动设置 position, 那么再写入的时候就会覆盖掉原先已经写入的字节,
            // 所以 rocketMQ 在这里手动指定可写的偏移量
            byteBuffer.position(currentPos);

            AppendMessageResult result;
            // 强转不同的消息, 最终逻辑还是交予 AppendMessageCallback 执行.
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
     *
     * @param data 消息内容
     * @return true-写入成功
     */
    public boolean appendMessage(final byte[] data) {
        // 当前可以开始写的位置量
        int currentPos = this.wrotePosition.get();
        // 不超过文件大小才可以写入
        if ((currentPos + data.length) <= this.fileSize) {
            try {
                // fileChannel 为磁盘文件的操作通道, 先设置它的可写位置值, 然后写入
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
     * 从 offset 开始写入 length 个数据
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        // 获取当前可写的位置
        int currentPos = this.wrotePosition.get();
        // 不超过文件大小才允许写入
        if ((currentPos + length) <= this.fileSize) {
            try {
                // 写入消息, 此时有可能写在操作系统 page cache中, 即还没落地到磁盘
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            // 写入成功后, 更新起始写位置
            this.wrotePosition.addAndGet(length);
            return true;
        }
        return false;
    }


    /**
     * 执行刷盘
     *
     * @param flushLeastPages 执行flush的最少页数, 一般取决于{@link MessageStoreConfig#getFlushCommitLogLeastPages()}
     * @return 已刷盘的最大位置
     */
    public int flush(final int flushLeastPages) {
        // 判断下是否可以刷盘
        if (this.isAbleToFlush(flushLeastPages)) {
            // 判断当前这个 MappedFile 是否可以被持有, 若可以将其引用计数+1
            if (this.hold()) {
                // 两种情况：当前可以写入的起始位置 or 当前已提交的位置
                int value = getReadPosition();
                try {
                    // 要么使用 fileChannel 刷盘, 要么使用 mappedByteBuffer 刷盘.
                    // 注意：若writeBuffer不为空, 那就得先 commit 再 flush, 不然存储在writeBuffer 的数据是不会进入到 FileChannel.
                    // 注意：调用 java.nio.MappedByteBuffer.force()方法并不会更新 ByteBuffer 里面的位置属性, 比如 position.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }
                // 更新已经刷盘的位置
                this.flushedPosition.set(value);
                // 将当前这个 MappedFile 引用计数减一, 如果可以释放便释放资源.
                this.release();
            } else {
                // 持有当前 MappedFile失败(原因：资源不可用, 或者它的引用计数小于等于0)
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    /**
     * 执行提交
     *
     * @param commitLeastPages 执行commit的最少页数, 一般取决于{@link MessageStoreConfig#getCommitCommitLogLeastPages()}
     * @return 已提交的最大位置
     */
    public int commit(final int commitLeastPages) {
        // 如果 writeBuffer 为空, 即没有开启直接缓冲区对象池的功能, 那么是没有 commit 这一概念的,
        // 此时只有 write 和 flush 的统计指标.
        if (writeBuffer == null) {
            // no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }
        // 允许提交操作
        if (this.isAbleToCommit(commitLeastPages)) {
            // 持有对象, 将其引用计数加1
            if (this.hold()) {
                // 执行commit
                commit0(commitLeastPages);
                // 引用计数减一
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
                // 将 writeBuffer 中的数据写入到 FileChannel 中.
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

    /**
     * 判断是否可以执行刷盘
     *
     * @param flushLeastPages 至少要刷入的页数
     * @return true-可以执行
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        // 最后一次刷盘的位置
        int flush = this.flushedPosition.get();
        // 两种情况：当前可以写入的位置 or 当前已提交的位置
        int write = getReadPosition();

        // 如果当前可以写入的位置已经等于底层磁盘文件规定的大小, 那么肯定可以执行刷盘.
        if (this.isFull()) {
            return true;
        }

        // flushLeastPages 大于0, 就将两个变量各自除以 OS_PAGE_SIZE 得到页码,
        // 相减等到的页数, 要大于等于参数指定要刷入的页数, 才允许刷盘
        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        // flushLeastPages 小于等于0, 只要可以刷盘便返回true.
        return write > flush;
    }

    /**
     * 判断是否可以执行 commit 操作
     *
     * @param commitLeastPages 执行commit的最少页数
     * @return true-可以执行
     */
    protected boolean isAbleToCommit(final int commitLeastPages) {
        // 已提交的位置
        int flush = this.committedPosition.get();
        // 已写入的位置
        int write = this.wrotePosition.get();
        // 已写满, 可以执行 commit 操作
        if (this.isFull()) {
            return true;
        }
        // 未 commit 的数据量要达到 commitLeastPages 指定的页数才允许commit,
        // 页数按照 page cache 来计算, 默认4kb.
        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }
        // 写入的位置大于刷盘的位置
        return write > flush;
    }

    /**
     * 已写入的数量已经等于文件规定的大小
     *
     * @return true-文件已写满
     */
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

    /**
     * 清理资源
     *
     * @param currentRef 本资源被引用的计数
     * @return true-清理成功
     */
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
        // 释放资源
        clean(this.mappedByteBuffer);
        // 将它们从计数Map中移除.
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    /**
     * 销毁当前的 MappedFile , 它会先调用{@link #cleanup(long)}方法清理资源, 然后直接把底层磁盘文件删除.
     *
     * @param intervalForcibly 时间间隔{@link ReferenceResource#shutdown(long)}
     * @return true-销毁成功
     */
    public boolean destroy(final long intervalForcibly) {
        // 父类的 shutdown() 用来清除一些引用计数和标志位,
        // 然后会再去调用 cleanup() 方法
        this.shutdown(intervalForcibly);
        if (this.isCleanupOver()) {
            try {
                // 关闭文件通道
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");
                long beginTime = System.currentTimeMillis();
                // 把文件删除.
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

    /**
     * 获取可读的最大位置, 详见代码注释.
     *
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        return this.writeBuffer == null ?
                // 如果 writeBuffer 为空, 说明没有使用 commit 概念, 那么返回的是当前可以写入的位置.
                this.wrotePosition.get() :
                // 如果 writeBuffer 不为空, 那么返回当前已提交的位置.
                this.committedPosition.get();
    }

    /**
     * 在磁盘中先将文件初始化好.
     *
     * @param type  刷盘方式
     * @param pages rocketMQ会用“0”填充整个文件, 这里表示每多少页刷盘一次.
     */
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();

        // 变量i每次循环都累加 MappedFile.OS_PAGE_SIZE, 因此它实际为这个配置的整数倍.
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            // 用0填充. 注意：如果使用 java.nio.ByteBuffer.put(int, byte) 添加的数据, 是不会影响
            // ByteBuffer自身维护的那几个变量的, 例如position、limit..
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                // 由于上面说过 i 是MappedFile.OS_PAGE_SIZE的整数倍, 所以除以这个值, 其实就是表示多少页.
                // 而flush等于上一次刷盘的 i, 所以这两个值相减, 就表示已经往 ByteBuffer 填充了多少页了.
                // 只要大于等于参数指定的值, 那么就强制刷盘
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc, 有待商榷：
            // https://stackoverflow.com/questions/53284031/why-thread-sleep0-can-prevent-gc-in-rocketmq
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

        // 当for循环完成时, 数据都已经添加到 ByteBuffer 中, 如果是同步刷盘, 那么就直接强制刷新.
        // 当然如果是异步刷新, 那么方法就直接返回了, 其它操作交由操作系统去刷盘.
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}", this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(), System.currentTimeMillis() - beginTime);
        // 将这个 MappedFile 的 mmap 映射锁定在物理内存中.
        this.mlock();
    }

    /**
     * 直接看{@link LibC#mlock(Pointer, NativeLong)}的注释.
     */
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

    /**
     * 直接看{@link LibC#munlock(Pointer, NativeLong)}的注释.
     */
    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
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

    @Override
    public String toString() {
        return this.fileName;
    }
}
