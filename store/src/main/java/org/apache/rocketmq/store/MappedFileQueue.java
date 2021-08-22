package org.apache.rocketmq.store;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * {@link MappedFile}是对单个磁盘文件的内存映射, 因此它管理一个文件.
 * {@link MappedFileQueue}用来管理{@link MappedFile}, 因此它管理一个目录, {@link #storePath}就是用来指定需要管理的目录.
 * <p>
 * MappedFileQueue 的偏移量与 MappedFile 的偏移量计算意义不一样：mappedFile的偏移量都是相对它自己, 但是mappedFileQueue的
 * 偏移量需要统计多个mappedFile, 所以它是绝对值, 会结合每个{@link MappedFile#getFileFromOffset()}来计算.
 */
public class MappedFileQueue {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    /**
     * 允许删除文件的最大个数
     */
    private static final int DELETE_FILES_BATCH_MAX = 10;

    /**
     * 文件存储目录, 比如可以是：commit log目录、consumer queue目录..等等
     */
    private final String storePath;

    /**
     * 管理的{@link MappedFile}的大小
     */
    private final int mappedFileSize;

    /**
     * 存储{@link MappedFile}的数据集合
     */
    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();

    /**
     * 用来提前创建{@link MappedFile}文件
     */
    private final AllocateMappedFileService allocateMappedFileService;

    /**
     * 保存每次刷盘的时间
     */
    private volatile long storeTimestamp = 0;

    /**
     * 已经刷盘的最终位置, 它是绝对值, 比如存在多个 MappedFile,
     * 它会把每个{@link MappedFile#getFileFromOffset()}也计算进去.
     */
    private long flushedWhere = 0;

    /**
     * 已经提交的最终位置, 它是绝对值, 比如存在多个 MappedFile,
     * 它会把每个{@link MappedFile#getFileFromOffset()}也计算进去.
     */
    private long committedWhere = 0;

    public MappedFileQueue(final String storePath, int mappedFileSize, AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    /**
     * 组件加载
     *
     * @return true-加载成功
     */
    public boolean load() {
        // 获取目录下的所有文件
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // rocketMQ设计的磁盘文件都是很有顺序的, 这边将其正序排序.
            // 这个很重要, 因为后续获取第一个MappedFile就是直接 mappedFiles.get(0), 所以顺序不能乱.
            Arrays.sort(files);
            for (File file : files) {
                // 磁盘文件大小不满足规定的 MappedFile 大小, 返回false, 表示加载失败.
                if (file.length() != this.mappedFileSize) {
                    log.warn(file + "\t" + file.length() + " length not matched message store config value, please check it manually");
                    return false;
                }
                try {
                    // 创建 MappedFile, 直接将它们的三个偏移量都置为最大值, 为啥可以这么处理？
                    // 因为如果有多个 MappedFile, 那肯定是只有最后一个 MappedFile 才需要恢复三个偏移量, 之前的 MappedFile 都已经写满,
                    // 肯定不需要恢复的. 后续恢复的逻辑位于：org.apache.rocketmq.store.DefaultMessageStore.recover()
                    MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);
                    mappedFile.setWrotePosition(this.mappedFileSize);
                    mappedFile.setFlushedPosition(this.mappedFileSize);
                    mappedFile.setCommittedPosition(this.mappedFileSize);
                    this.mappedFiles.add(mappedFile);
                    log.info("load " + file.getPath() + " OK");
                } catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * 提交日志操作
     *
     * @param commitLeastPages 指定至少需要提交的页数, 若为0表示不限制. 可以参考
     *                         {@link MessageStoreConfig#getCommitCommitLogLeastPages()}
     * @return true-表示没有数据commit, false-表示有部分数据已经commit.
     */
    public boolean commit(final int commitLeastPages) {
        boolean result = true;
        // 通过已经commit的偏移量找到对应的 MappedFile
        MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
        if (mappedFile != null) {
            // 若能找到, 直接调用该 mappedFile 的 commit(), 该方法会返回最大提交的位置
            int offset = mappedFile.commit(commitLeastPages);
            // 理论上此时已经提交的偏移量
            long where = mappedFile.getFileFromOffset() + offset;
            // 如果 where == this.committedWhere 表示并没有 commit 什么数据.
            // 只有这两个不等, 才说明有数据被提交了.
            result = (where == this.committedWhere);
            // 更新当前 MappedFileQueue 的 committedWhere 偏移量
            this.committedWhere = where;
        }
        return result;
    }

    /**
     * 刷盘日志操作
     *
     * @param flushLeastPages 指定至少需要刷入的页数, 若为0表示不限制. 可以参考
     *                        {@link MessageStoreConfig#getFlushCommitLogLeastPages()}
     * @return true-表示没有数据flush, false-表示有部分数据已经flush.
     */
    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        // 通过已经commit的偏移量找到对应的 MappedFile
        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
        if (mappedFile != null) {
            long tmpTimeStamp = mappedFile.getStoreTimestamp();

            // 若能找到, 直接调用该 mappedFile 的 flush(), 该方法会返回已经刷盘的最大位置
            int offset = mappedFile.flush(flushLeastPages);
            // 理论上此时已经刷盘的偏移量
            long where = mappedFile.getFileFromOffset() + offset;
            // 如果 where == this.flushedWhere 表示并没有 flush 什么数据.
            // 只有这两个不等, 才说明有数据被刷盘了.
            result = where == this.flushedWhere;
            this.flushedWhere = where;
            if (0 == flushLeastPages) {
                // 只有全部数据都刷盘以后才会更新 storeTimestamp
                this.storeTimestamp = tmpTimeStamp;
            }
        }
        return result;
    }

    public void shutdown(final long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    /**
     * 获取磁盘文件修改时间大于等于指定时间的{@link MappedFile},
     * 如果都没有找到, 会默认返回集合最后一个.
     *
     * @param timestamp 时间戳
     * @return 映射文件
     */
    public MappedFile getMappedFileByTime(final long timestamp) {
        // 拷贝 mappedFile 对象数组
        Object[] mfs = this.copyMappedFiles(0);
        if (null == mfs) return null;
        // 寻找第一个底层磁盘文件修改时间大于等于指定时间戳的 MappedFile
        for (int i = 0; i < mfs.length; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }
        // 如果for循环没有找到指定时间戳的文件, 兜底返回最后一个.
        return (MappedFile) mfs[mfs.length - 1];
    }

    /**
     * 获取第一个 mapped file, 直接从{@link #mappedFiles}取第一个
     *
     * @return 可能为null
     */
    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;
        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }
        return mappedFileFirst;
    }

    /**
     * 获取最后一个 mapped file, 如果文件不存在或者文件已写完, 那么创建一个新的
     *
     * @param startOffset 开始偏移量
     * @return 内存映射文件
     */
    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    /**
     * 获取最后一个 mapped file, 如果文件不存在或者文件已写完, 那么根据参数决定是否要创建新的.
     *
     * @param startOffset 开始偏移量
     * @param needCreate  true-文件不存在或者已写满, 需要创建新的 mapped file
     * @return 内存映射文件
     */
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        // 用来作为新 mapped file 的文件名
        long createOffset = -1;

        // 获取当前的最后一个文件, 可能为null
        MappedFile mappedFileLast = getLastMappedFile();

        // 如果为空, 那么就根据参数 startOffset, 计算出文件名(也就是起始偏移量)
        if (mappedFileLast == null) {
            // 减去余数
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }

        // 如果当前最后一个文件不为空, 但是它已经写满了, 那么也需要创建一个新, 文件名直接累加.
        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }

        // 需要创建新的 mapped file
        if (createOffset != -1 && needCreate) {

            // 新文件的路径
            String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
            MappedFile mappedFile = null;

            if (this.allocateMappedFileService != null) {
                // 创建一个 MappedFile 需要一定的时间, 为了提高 putMessage 的效率, 除了创建当前文件以外, 还会顺带创建下一个相邻的文件, 暂且称它为“预写”.
                // 这样子, 等下一次 putMessage 需要下一个相邻的文件时, 这个方法就可以直接返回, 而不需要从新开始进行内存映射操作.
                String nextNextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset + this.mappedFileSize);
                mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath, nextNextFilePath, this.mappedFileSize);
            } else {
                // 没开启预写功能, 就只会创建当前需要的文件
                try {
                    mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
                } catch (IOException e) {
                    log.error("create mappedFile exception", e);
                }
            }
            if (mappedFile != null) {
                // 当前仅当 mappedFiles 为空时, 才会去设置 mappedFile 的 firstCreateInQueue 为true
                // 表示它是第一个创建的
                if (this.mappedFiles.isEmpty()) {
                    mappedFile.setFirstCreateInQueue(true);
                }
                // 然后将它加入到集合中
                this.mappedFiles.add(mappedFile);
            }
            return mappedFile;
        }
        return mappedFileLast;
    }

    /**
     * 获取最后一个 mapped file, 直接从{@link #mappedFiles}取末尾
     *
     * @return 可能为null
     */
    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;
        // 循环查询, 区别于 getFirstMappedFile(), 三种情况下会返回null:
        // 1.mappedFiles数据集合为空
        // 2.mappedFiles索引i所在的对象为空
        // 3.出现未知异常(情况较少)
        while (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }
        return mappedFileLast;
    }

    /**
     * 通过偏移量查询{@link MappedFile}
     *
     * @param offset 指定的偏移量
     * @return 可能为null (when not found and returnFirstOnNotFound is <code>false</code>).
     */
    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    /**
     * 通过偏移量查询{@link MappedFile}
     *
     * @param offset                绝对的偏移量, 即算上了每个{@link MappedFile#getFileFromOffset()}
     * @param returnFirstOnNotFound 如果未找到映射文件, 是否要返回第一个
     * @return 可能为null (when not found and returnFirstOnNotFound is <code>false</code>).
     */
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            // 获取第一个和最后一个 MappedFile
            MappedFile firstMappedFile = this.getFirstMappedFile();
            MappedFile lastMappedFile = this.getLastMappedFile();
            // 如果这两个文件任意一个为空, 则方法返回null
            if (Objects.isNull(firstMappedFile) || Objects.isNull(lastMappedFile)) {
                return null;
            }
            // offset能查询的范围：第一个文件的起始偏移量, 最后一个文件的起始偏移量 + 文件的大小
            if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                // 不符合偏移量查询区间限制, 要么返回null, 要么根据参数 returnFirstOnNotFound 是否要返回第一个 mapped file
                LOG_ERROR.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}", offset, firstMappedFile.getFileFromOffset(), lastMappedFile.getFileFromOffset() + this.mappedFileSize, this.mappedFileSize, this.mappedFiles.size());
            } else {
                // 解释一下这边为何要这样子计算, 假设一个文件大小为1KB, 按照rocketMQ设计, 它的底层磁盘文件应该为：00000000、00001024、00002048、00003072....
                // 那么在创建这个类的时候, 它就会把这些文件按照升序的顺序(具体逻辑看load()方法), 保存到 mappedFiles 中.
                // 假设现在要查询 offset=1552 所在的 MappedFile文件, 目测一下肯定在 00001024 文件上, 即index=1. 所以我们需要这样计算：1552/1024 - 0/1024 = 1.
                // offset / this.mappedFileSize 是为了确定该 offset 逻辑上在第几个 mappedFile（从0开始计算）
                // firstMappedFile.getFileFromOffset() / this.mappedFileSize 是为了确定当前第一个 mappedFile 所在的位置.
                // 相减一下就得到 offset 位于集合中的哪一个 MappedFile 上.
                int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                MappedFile targetFile = null;
                try {
                    // 从集合中找出目标文件
                    targetFile = this.mappedFiles.get(index);
                } catch (Exception ignored) {
                }
                // 符合条件, 直接返回.
                if (targetFile != null && offset >= targetFile.getFileFromOffset() && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
                    return targetFile;
                }
                // 代码能走到这边, 说明 targetFile 要么为空, 要么被更改了导致偏移量不对.
                for (MappedFile tmpMappedFile : this.mappedFiles) {
                    // 执行兜底方案, 直接遍历, 一个一个查询.
                    if (offset >= tmpMappedFile.getFileFromOffset() && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                        return tmpMappedFile;
                    }
                }
            }
            // 如果还是没有找到, 根据查询参数选择性返回
            if (returnFirstOnNotFound) {
                return firstMappedFile;
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }
        // 啥都没找到就返回null.
        return null;
    }

    // TODO
    public boolean resetOffset(long offset) {
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast != null) {
            long lastOffset = mappedFileLast.getFileFromOffset() + mappedFileLast.getWrotePosition();
            long diff = lastOffset - offset;

            final int maxDiff = this.mappedFileSize * 2;
            if (diff > maxDiff) return false;
        }

        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator();

        while (iterator.hasPrevious()) {
            mappedFileLast = iterator.previous();
            if (offset >= mappedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                iterator.remove();
            }
        }
        return true;
    }

    /**
     * 获取最小偏移量, 其实就是第一个文件的起始偏移量
     *
     * @return 异常时返回-1
     */
    public long getMinOffset() {
        if (!this.mappedFiles.isEmpty()) {
            try {
                return this.mappedFiles.get(0).getFileFromOffset();
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }

    /**
     * 获取最大偏移量, 其实就是返回最后一个文件的起始偏移量 + 最大可读偏移量
     *
     * @return 异常时返回0
     */
    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    /**
     * 获取最大已写入的偏移量, 其实就是返回最后一个文件的起始偏移量 + 它的最大可写位置
     *
     * @return 异常时返回0
     */
    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    /**
     * 自检查, 一般是害怕磁盘文件被人为改变.
     */
    public void checkSelf() {
        if (!this.mappedFiles.isEmpty()) {
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            MappedFile pre = null;
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (pre != null) {
                    // 因为 rocketMQ 设计的磁盘文件都是以offset命名的, 且都是紧邻的偏移量.
                    // 比如文件大小若设置为1M, 那么第一个文件取名为"0000000000", 当它写满以后, 第二个文件取名为"0001048576"...以此类推.
                    // 所以第二个文件的起始偏移量(实际就是文件名)减去第一个文件的起始偏移量, 说明文件被改过且已经不符合 rocketMQ 规定的格式.
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}", pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }

    /**
     * 用来当 rocketMQ 重启时, 分析之前写入磁盘的{@link MappedFile}, 进而恢复它们的
     * wrotePosition、committedPosition、wrotePosition
     *
     * @param offset 有效的偏移量, 这个offset是一个累加值. 比如存在5个磁盘文件, 那么这个offset是第5个文件有效的偏移量,
     *               它会累加上前4个磁盘文件的 fileSize.
     */
    public void truncateDirtyFiles(long offset) {
        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();
        for (MappedFile file : this.mappedFiles) {
            // 所有每个 MappedFile 它的最大偏移量
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            // 如果最大偏移量大于 offset, 说明这个 offset 就位于这个 MappedFile 中.
            // 相反地, 如果小于 offset, 所以这个 MappedFile 是位于靠前的文件, 那么就跳过.
            if (fileTailOffset > offset) {
                // 既然已经确定 offset 位于找到的 MappedFile 中, 那么它肯定要大于起始偏移量
                if (offset >= file.getFileFromOffset()) {
                    // 取余, 是为了消除之前统计前几个磁盘文件累加的偏移量, 得到的余数就等于
                    // 此 offset 相对于它所在的 MappedFile 的逻辑偏移量.
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {
                    // 非法的文件, 直接删除掉.
                    file.destroy(1000);
                    willRemoveFiles.add(file);
                }
            }
        }
        this.deleteExpiredFile(willRemoveFiles);
    }

    /**
     * 从{@link #mappedFiles}中删除指定的文件.
     *
     * @param files 待删除的文件集合.
     */
    void deleteExpiredFile(List<MappedFile> files) {
        if (files.isEmpty()) {
            return;
        }
        Iterator<MappedFile> iterator = files.iterator();
        while (iterator.hasNext()) {
            MappedFile cur = iterator.next();
            if (!this.mappedFiles.contains(cur)) {
                iterator.remove();
                log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
            }
        }
        try {
            if (!this.mappedFiles.removeAll(files)) {
                log.error("deleteExpiredFile remove failed.");
            }
        } catch (Exception e) {
            log.error("deleteExpiredFile has exception.", e);
        }
    }

    /**
     * 物理删除最后一个{@link MappedFile}
     */
    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            lastMappedFile.destroy(1000);
            this.mappedFiles.remove(lastMappedFile);
            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());
        }
    }

    public int deleteExpiredFileByTime(final long expiredTime, final int deleteFilesInterval, final long intervalForcibly, final boolean cleanImmediately) {
        Object[] mfs = this.copyMappedFiles(0);
        if (null == mfs) return 0;
        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        List<MappedFile> files = new ArrayList<MappedFile>();
        if (null != mfs) {
            for (int i = 0; i < mfsLength; i++) {
                MappedFile mappedFile = (MappedFile) mfs[i];
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                    if (mappedFile.destroy(intervalForcibly)) {
                        files.add(mappedFile);
                        deleteCount++;

                        if (files.size() >= DELETE_FILES_BATCH_MAX) {
                            break;
                        }

                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    //avoid deleting files in the middle
                    break;
                }
            }
        }
        deleteExpiredFile(files);
        return deleteCount;
    }

    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy;
                MappedFile mappedFile = (MappedFile) mfs[i];
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset " + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }

                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MappedFile> tmpFiles = new ArrayList<MappedFile>();
                    tmpFiles.add(mappedFile);
                    this.deleteExpiredFile(tmpFiles);
                } else {
                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }

    /**
     * 计算还有多少字节的数据未提交, 就是用已写入位置 - 已提交位置
     *
     * @return 字节数
     */
    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - committedWhere;
    }

    /**
     * 计算还有多少字节的数据未刷盘, 就是最大可读数据(最大可写 or 最大已提交) - 已刷盘的位置
     *
     * @return 字节数
     */
    public long remainHowManyDataToFlush() {
        return getMaxOffset() - flushedWhere;
    }

    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty()) return 0;

        long committed = this.flushedWhere;
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMappedFile(0, false);
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
            }
        }

        return 0;
    }

    public long getMappedMemorySize() {
        long size = 0;
        Object[] mfs = this.copyMappedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }
        return size;
    }

    /**
     * 拷贝{@link MappedFile}文件集合
     *
     * @param reservedMappedFiles 当原文件集合大于此值才会拷贝
     * @return 新的 MappedFile 对象数据
     */
    private Object[] copyMappedFiles(final int reservedMappedFiles) {
        Object[] mfs;
        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }
        mfs = this.mappedFiles.toArray();
        return mfs;
    }

    /* getter、setter*/

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(final long committedWhere) {
        this.committedWhere = committedWhere;
    }
}
