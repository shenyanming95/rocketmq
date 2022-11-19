package org.apache.rocketmq.store.index;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * {@link IndexFile}的处理类, 这一类型的索引一般用于运维搜索消息
 */
public class IndexService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * 尝试创建索引文件的最大次数
     */
    private static final int MAX_TRY_IDX_CREATE = 3;

    /**
     * 存储引擎的实现
     */
    private final DefaultMessageStore defaultMessageStore;

    /**
     * 哈希槽的个数, 详见{@link IndexFile#hashSlotNum}
     */
    private final int hashSlotNum;

    /**
     * 索引条目的个数, 详见{@link IndexFile#indexNum}
     */
    private final int indexNum;

    /**
     * indexFile文件存储路径, ${data_home}/index/
     */
    private final String storePath;

    /**
     * 用来维护索引文件的集合
     */
    private final ArrayList<IndexFile> indexFileList = new ArrayList<IndexFile>();

    /**
     * 读写锁, 用来对上面索引文件集合熟悉的并发操作
     */
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public IndexService(final DefaultMessageStore store) {
        this.defaultMessageStore = store;
        this.hashSlotNum = store.getMessageStoreConfig().getMaxHashSlotNum();
        this.indexNum = store.getMessageStoreConfig().getMaxIndexNum();
        this.storePath = StorePathConfigHelper.getStorePathIndex(store.getMessageStoreConfig().getStorePathRootDir());
    }

    /**
     * 加载
     *
     * @param lastExitOK 上一次退出rocketMQ是否属于正常退出
     * @return true-加载成功
     */
    public boolean load(final boolean lastExitOK) {
        // 获取所有索引文件
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // 索引文件的创建是按照时间戳, 这边将其按照创建时间正序排序
            Arrays.sort(files);
            for (File file : files) {
                try {
                    // 创建 indexFile 文件, 然后加载, 实现逻辑就是获取索引文件头.
                    IndexFile f = new IndexFile(file.getPath(), this.hashSlotNum, this.indexNum, 0, 0);
                    f.load();

                    // 非正常退出, 判断indexFile最后一条消息存储的时间, 是否大于checkpoint记录的index文件最终刷盘的时间.
                    // 如果大于它, 说明这个 indexFile 属于脏数据, 将其清除.
                    if (!lastExitOK) {
                        if (f.getEndTimestamp() > this.defaultMessageStore.getStoreCheckpoint().getIndexMsgTimestamp()) {
                            f.destroy(0);
                            continue;
                        }
                    }
                    log.info("load index file OK, " + f.getFileName());
                    this.indexFileList.add(f);
                } catch (IOException e) {
                    log.error("load file {} error", file, e);
                    return false;
                } catch (NumberFormatException e) {
                    log.error("load file {} error", file, e);
                }
            }
        }
        return true;
    }

    /**
     * 单个索引文件刷盘
     */
    public void flush(final IndexFile f) {
        if (null == f) return;

        // 用来判断索引文件是否已写满, 如果已写满那么就拿到它最后一次存储消息的时间
        long indexMsgTimestamp = 0;
        if (f.isWriteFull()) {
            indexMsgTimestamp = f.getEndTimestamp();
        }
        // 刷盘
        f.flush();
        // 如果索引文件已写满, 那么就将该索引文件最后一次存储消息的时间保存到checkpoint文件中.
        if (indexMsgTimestamp > 0) {
            this.defaultMessageStore.getStoreCheckpoint().setIndexMsgTimestamp(indexMsgTimestamp);
            this.defaultMessageStore.getStoreCheckpoint().flush();
        }
    }

    public void destroy() {
        try {
            this.readWriteLock.writeLock().lock();
            for (IndexFile f : this.indexFileList) {
                f.destroy(1000 * 3);
            }
            this.indexFileList.clear();
        } catch (Exception e) {
            log.error("destroy exception", e);
        } finally {
            this.readWriteLock.writeLock().unlock();
        }
    }

    public void start() {
    }

    public void shutdown() {
    }

    /**
     * 删除过期的文件
     *
     * @param offset 小于此偏移量的文件都会被删除
     */
    public void deleteExpiredFile(long offset) {
        Object[] files = null;
        try {
            // 加读锁, 获取
            this.readWriteLock.readLock().lock();
            if (this.indexFileList.isEmpty()) {
                return;
            }
            // 因为在执行load()方法时, 对集合indexFileList就已经排过序, 它的第一个文件就是最早最早创建的.
            // 如果它记录的最大的commitlog偏移量小于参数offset, 那么这一批索引文件都需要被删除.
            long endPhyOffset = this.indexFileList.get(0).getEndPhyOffset();
            if (endPhyOffset < offset) {
                files = this.indexFileList.toArray();
            }
        } catch (Exception e) {
            log.error("destroy exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        // 删除文件
        if (files != null) {
            List<IndexFile> fileList = new ArrayList<IndexFile>();
            for (int i = 0; i < (files.length - 1); i++) {
                IndexFile f = (IndexFile) files[i];
                if (f.getEndPhyOffset() < offset) {
                    fileList.add(f);
                } else {
                    break;
                }
            }
            this.deleteExpiredFile(fileList);
        }
    }

    /**
     * 删除过期文件
     *
     * @param files 待删除的文件集合
     */
    private void deleteExpiredFile(List<IndexFile> files) {
        if (!files.isEmpty()) {
            try {
                // 对集合数据有变动, 则加写锁
                this.readWriteLock.writeLock().lock();
                for (IndexFile file : files) {
                    boolean destroyed = file.destroy(3000);
                    destroyed = destroyed && this.indexFileList.remove(file);
                    if (!destroyed) {
                        log.error("deleteExpiredFile remove failed.");
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }
        }
    }

    /**
     * 根据key(消息关键字)来搜索消息
     *
     * @param topic  主题
     * @param key    要搜索的消息关键字
     * @param maxNum 要搜索的最大条数
     * @param begin  搜索条件-开始时间
     * @param end    搜索条件-终止时间
     * @return 搜索结果
     */
    public QueryOffsetResult queryOffset(String topic, String key, int maxNum, long begin, long end) {
        // 用来存储匹配的commitlog物理偏移量
        List<Long> phyOffsets = new ArrayList<Long>(maxNum);

        // 当前索引文件存储的最大的消息存储时间戳、最大的commitlog物理偏移量
        long indexLastUpdateTimestamp = 0;
        long indexLastUpdatePhyoffset = 0;

        // 最大能查询的消息条数
        maxNum = Math.min(maxNum, this.defaultMessageStore.getMessageStoreConfig().getMaxMsgsNumBatch());
        try {
            // 加读锁
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {
                // 从集合后面往前找
                for (int i = this.indexFileList.size(); i > 0; i--) {
                    IndexFile f = this.indexFileList.get(i - 1);

                    // 如果是最后一个索引文件, 获取它的最大存储时间戳和最大commitlog偏移量
                    boolean lastFile = i == this.indexFileList.size();
                    if (lastFile) {
                        indexLastUpdateTimestamp = f.getEndTimestamp();
                        indexLastUpdatePhyoffset = f.getEndPhyOffset();
                    }

                    // 消息位于此文件中, 它是根据时间来匹配的.
                    if (f.isTimeMatched(begin, end)) {
                        // 从文件捞出符合条件的索引数据, 将它保存的物理偏移量添加到集合中
                        f.selectPhyOffset(phyOffsets, buildKey(topic, key), maxNum, begin, end, lastFile);
                    }

                    // 因为这里是倒序循环, 如果索引文件的起始时间戳小于参数要查询的开始时间, 那么没有必要继续循环
                    if (f.getBeginTimestamp() < begin) {
                        break;
                    }
                    // 达到最大数量也没有必要循环
                    if (phyOffsets.size() >= maxNum) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("queryMsg exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }
        // 返回结果
        return new QueryOffsetResult(phyOffsets, indexLastUpdateTimestamp, indexLastUpdatePhyoffset);
    }

    /**
     * 用于构建索引文件用的key
     */
    private String buildKey(final String topic, final String key) {
        return topic + "#" + key;
    }

    /**
     * 构建索引文件, 一般在重启恢复 commit log 文件时被调用, 为恢复好的 commit log 文件生成索引文件
     *
     * @param req 已恢复好的 commit log 文件信息
     */
    public void buildIndex(DispatchRequest req) {
        // 获取当前可以用的索引文件
        IndexFile indexFile = retryGetAndCreateIndexFile();
        // 文件可用的情况下才会构建索引
        if (indexFile != null) {
            long endPhyOffset = indexFile.getEndPhyOffset();
            DispatchRequest msg = req;
            String topic = msg.getTopic();
            String keys = msg.getKeys();
            if (msg.getCommitLogOffset() < endPhyOffset) {
                return;
            }

            // 事务回滚消息不用构建
            final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    break;
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    return;
            }

            /*
             * RocketMQ 会为每条 message 的 uniqKey 和用户自定义的 key 生成索引.
             */

            // 如果消息自带了唯一键, 为其创建一条索引
            if (req.getUniqKey() != null) {
                indexFile = putKey(indexFile, msg, buildKey(topic, req.getUniqKey()));
                if (indexFile == null) {
                    log.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                    return;
                }
            }

            // 为消息中的每个key都创建一条索引
            if (keys != null && keys.length() > 0) {
                String[] keyset = keys.split(MessageConst.KEY_SEPARATOR);
                for (int i = 0; i < keyset.length; i++) {
                    String key = keyset[i];
                    if (key.length() > 0) {
                        indexFile = putKey(indexFile, msg, buildKey(topic, key));
                        if (indexFile == null) {
                            log.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                            return;
                        }
                    }
                }
            }
        } else {
            log.error("build index error, stop building index");
        }
    }

    /**
     * 将commit log消息存储到索引文件中
     *
     * @param indexFile 存入到这个索引文件中
     * @param msg       消息内容
     * @param idxKey    消息的唯一键, 就会用它算哈希, 然后添加到索引文件中
     * @return 非null表示成功
     */
    private IndexFile putKey(IndexFile indexFile, DispatchRequest msg, String idxKey) {
        // 将 commit log 消息写入到索引文件中, 如果写满了会尝试重新创建一个新的索引文件再去写
        for (boolean ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp()); !ok; ) {
            log.warn("Index file [" + indexFile.getFileName() + "] is full, trying to create another one");
            // 代码走到这里, 说明for循环的方法调用返回false, 即索引创建失败, 这边就会重新再创建一个
            indexFile = retryGetAndCreateIndexFile();
            if (null == indexFile) {
                return null;
            }
            // 然后尝试再塞入消息
            ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp());
        }
        return indexFile;
    }

    /**
     * 重试获取或创建索引文件, 这个方法在{@link #getAndCreateLastIndexFile()}基础上加了重试的逻辑而已.
     *
     * @return {@link IndexFile} or null on failure.
     */
    public IndexFile retryGetAndCreateIndexFile() {
        IndexFile indexFile = null;

        for (int times = 0; null == indexFile && times < MAX_TRY_IDX_CREATE; times++) {
            // 获取当前最后一个索引文件, 若当前最后一个索引文件也写满了, 这个方法会创建一个新的索引文件, 作为最新的最后一个索引文件
            indexFile = this.getAndCreateLastIndexFile();
            // 能获取到就直接跳出循环
            if (null != indexFile) break;

            // 获取不到就等待1s后再次获取
            try {
                log.info("Tried to create index file " + times + " times");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            }
        }

        // 写入系统标志
        if (null == indexFile) {
            this.defaultMessageStore.getAccessRights().makeIndexFileError();
            log.error("Mark index file cannot build flag");
        }

        return indexFile;
    }

    /**
     * 获取最后一个索引文件, 如果它写满了, 那么会创建一个新的文件再返回.
     */
    public IndexFile getAndCreateLastIndexFile() {
        IndexFile indexFile = null;
        IndexFile prevIndexFile = null;
        long lastUpdateEndPhyOffset = 0;
        long lastUpdateIndexTimestamp = 0;

        {
            // 加读锁
            this.readWriteLock.readLock().lock();
            // 原先就已经存在索引文件的情况
            if (!this.indexFileList.isEmpty()) {
                // 直接拿到当前最后一个索引文件
                IndexFile tmp = this.indexFileList.get(this.indexFileList.size() - 1);
                if (!tmp.isWriteFull()) {
                    // 如果它还没有写满, 那么可以返回它
                    indexFile = tmp;
                } else {
                    // 如果它已经写满, 将它的信息记录下来
                    lastUpdateEndPhyOffset = tmp.getEndPhyOffset();
                    lastUpdateIndexTimestamp = tmp.getEndTimestamp();
                    prevIndexFile = tmp;
                }
            }
            // 释放读锁
            this.readWriteLock.readLock().unlock();
        }

        // 由上面代码块可以知道, 如果 indexFile 为空, 说明当前已存在的最后一个索引文件已经写满了, 或者当前压根就没有索引文件.
        // 处于这两种情况就需要重新创建一个IndexFile.
        if (indexFile == null) {
            try {
                // 创建一个新的索引文件, 文件名就是当前时间戳
                String fileName = this.storePath + File.separator + UtilAll.timeMillisToHumanString(System.currentTimeMillis());
                indexFile = new IndexFile(fileName, this.hashSlotNum, this.indexNum, lastUpdateEndPhyOffset, lastUpdateIndexTimestamp);
                // 加写锁后将其写入到集合中
                this.readWriteLock.writeLock().lock();
                this.indexFileList.add(indexFile);
            } catch (Exception e) {
                log.error("getLastIndexFile exception ", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }

            // 开启一个后台线程, 将上一个已经写满的文件刷盘
            if (indexFile != null) {
                final IndexFile flushThisFile = prevIndexFile;
                Thread flushThread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        IndexService.this.flush(flushThisFile);
                    }
                }, "FlushIndexFileThread");

                flushThread.setDaemon(true);
                flushThread.start();
            }
        }

        return indexFile;
    }

}
