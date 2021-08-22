package org.apache.rocketmq.store;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;

import java.io.File;
import java.io.IOException;
import java.util.ServiceLoader;
import java.util.concurrent.*;

/**
 * 这个类是用来提前创建{@link MappedFile}, 它的工作流程如下：
 * 1.该类本身是一个服务线程, 它会不断地从{@link #requestQueue}中取出请求{@link AllocateRequest}, 为其生成{@link MappedFile},
 *   满足一定条件, 还会预先刷入磁盘, 值为都为“0”.
 * 2.提供{@link #putRequestAndReturnMappedFile(String, String, int)}方法供外部添加请求到队列中.
 */
public class AllocateMappedFileService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 每个{@link AllocateRequest}默认都会创建一个{@link AllocateRequest#countDownLatch}, 这个属性用在等待创建 MappedFile 时,
     * 要等待的最大时间, 单位是毫秒.
     */
    private static int waitTimeOut = 1000 * 5;

    /**
     * 用来映射文件路径和请求, 存储格式为：<filePath, AllocateRequest>.
     * 如果是 AllocateRequest 是第一次添加到这个Map中, 那么它会被加入到{@link #requestQueue}, 后续由专门线程去创建 MappedFile.
     * 若 AllocateRequest 不是第一次添加, 它会直接获取执行结果, 就不会添加到{@link #requestQueue}.
     */
    private ConcurrentMap<String, AllocateRequest> requestTable = new ConcurrentHashMap<String, AllocateRequest>();

    /**
     * 优先级队列, 一般{@link AllocateRequest#fileSize}越大, 优先级越高, 意味着这个请求会更快地被处理.
     * {@link #run()}方法每次会从这个队列取出请求来处理.
     */
    private PriorityBlockingQueue<AllocateRequest> requestQueue = new PriorityBlockingQueue<AllocateRequest>();

    /**
     * 如果创建过程中出现异常了, 这个属性就会被置为true
     */
    private volatile boolean hasException = false;

    /**
     * 主要是为了获取一些配置
     */
    private DefaultMessageStore messageStore;

    public AllocateMappedFileService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    /**
     * 添加一个预热文件的请求, 然后等待其预热好的{@link MappedFile}, 怎么理解这个方法?
     * 首先, rocketMQ 的磁盘文件是有序的, 我们假设第一个预热请求A, 它需要创建C1文件, 同时会预写C2文件;
     * 第二个请求B过来, 它需要创建C2文件, 同时会预写C3文件. 此时, C2文件已经在第一个预热请求A中就创建好了,
     * 因此预热请求B中, 参加C2文件就不会添加到优先级队列{@link #requestQueue}中, 而是直接获取结果
     *
     * @param nextFilePath     第一个需要预热的文件路径
     * @param nextNextFilePath 第二个需要预热的文件路径
     * @param fileSize         文件大小
     * @return 内存映射文件
     */
    public MappedFile putRequestAndReturnMappedFile(String nextFilePath, String nextNextFilePath, int fileSize) {
        // 参数有两个路径, 所以默认可以提交的数量就为2.
        int canSubmitRequests = 2;
        // isTransientStorePoolEnable()为true有3个条件：开启缓冲池功能 + 异步刷盘 + master broker.
        if (this.messageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            // 如果从 transientStorePool中获取 ByteBuffer 支持快速失败, 并且当前 broker 是 master 角色.
            if (this.messageStore.getMessageStoreConfig().isFastFailIfNoBufferInStorePool()
                    && BrokerRole.SLAVE != this.messageStore.getMessageStoreConfig().getBrokerRole()) {
                // if broker is slave, don't fast fail even no buffer in pool.
                // canSubmitRequests, 就等于池中可用的 byteBuffer 数量减去请求队列的大小.
                canSubmitRequests = this.messageStore.getTransientStorePool().availableBufferNums() - this.requestQueue.size();
            }
        }

        // 开始添加第一个预热文件请求.
        AllocateRequest nextReq = new AllocateRequest(nextFilePath, fileSize);
        // 如果原先 requestTable 不存在这个路径的预热请求, 那么就会返回null, 否则返回的已经存在的预热请求.
        // 因此, 只有在第一次加入到 requestTable 的情况下才会进入下面这个if语句块, 即添加到优先级队列等待创建 MappedFile 文件.
        boolean nextPutOK = this.requestTable.putIfAbsent(nextFilePath, nextReq) == null;
        if (nextPutOK) {
            // canSubmitRequests 为负数, 说明缓冲池不够了
            if (canSubmitRequests <= 0) {
                log.warn("[NOTIFYME]TransientStorePool is not enough, so create mapped file error, " + "RequestQueueSize : {}, StorePoolSize: {}",
                        this.requestQueue.size(), this.messageStore.getTransientStorePool().availableBufferNums());
                // 删除刚刚加入的key-value
                this.requestTable.remove(nextFilePath);
                return null;
            }
            // 将其加入到优先级队列中.
            boolean offerOK = this.requestQueue.offer(nextReq);
            if (!offerOK) {
                log.warn("never expected here, add a request to preallocate queue failed");
            }
            // 可提交次数减一
            canSubmitRequests--;
        }

        // 跟上一段代码同样的操作
        AllocateRequest nextNextReq = new AllocateRequest(nextNextFilePath, fileSize);
        boolean nextNextPutOK = this.requestTable.putIfAbsent(nextNextFilePath, nextNextReq) == null;
        if (nextNextPutOK) {
            if (canSubmitRequests <= 0) {
                log.warn("[NOTIFYME]TransientStorePool is not enough, so skip preallocate mapped file, " + "RequestQueueSize : {}, StorePoolSize: {}",
                        this.requestQueue.size(), this.messageStore.getTransientStorePool().availableBufferNums());
                this.requestTable.remove(nextNextFilePath);
            } else {
                boolean offerOK = this.requestQueue.offer(nextNextReq);
                if (!offerOK) {
                    log.warn("never expected here, add a request to preallocate queue failed");
                }
            }
        }

        // 出现异常了, 这个值会在这个类的 run() 中被更新. 一般是在创建磁盘文件出现I/O异常时被置为true.
        if (hasException) {
            log.warn(this.getServiceName() + " service has exception. so return null");
            return null;
        }

        // 因为实际创建 MappedFile 的逻辑放在 run() 中, 即异步创建. 所以调用方必须要这里阻塞等待一段时间.
        AllocateRequest result = this.requestTable.get(nextFilePath);
        try {
            if (result != null) {
                // 返回true, 表示计数器已经被置为0, 说明已经创建好了.
                // 返回false, 说明超时返回.
                boolean waitOK = result.getCountDownLatch().await(waitTimeOut, TimeUnit.MILLISECONDS);
                if (!waitOK) {
                    log.warn("create mmap timeout " + result.getFilePath() + " " + result.getFileSize());
                    return null;
                } else {
                    // 创建成功, 清除资源.
                    this.requestTable.remove(nextFilePath);
                    return result.getMappedFile();
                }
            } else {
                log.error("find preallocate mmap failed, this never happen");
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
        }

        return null;
    }

    @Override
    public String getServiceName() {
        return AllocateMappedFileService.class.getSimpleName();
    }

    @Override
    public void shutdown() {
        super.shutdown(true);
        for (AllocateRequest req : this.requestTable.values()) {
            if (req.mappedFile != null) {
                log.info("delete pre allocated maped file, {}", req.mappedFile.getFileName());
                req.mappedFile.destroy(1000);
            }
        }
    }

    public void run() {
        log.info(this.getServiceName() + " service started");
        while (!this.isStopped() && this.mmapOperation()) {
            // 如果线程启动, 会一直调用 mmapOperation() 方法.
            // 直到此线程被停止, 或者 mmapOperation()方法返回false.
        }
        log.info(this.getServiceName() + " service end");
    }

    /**
     * 预先生成{@link MappedFile}文件, 满足一定条件还会生成对应的磁盘文件.
     *
     * @return Only interrupted by the external thread, will return false
     */
    private boolean mmapOperation() {
        boolean isSuccess = false;
        AllocateRequest req = null;
        try {
            // 从优先级队列中取出请求对象
            req = this.requestQueue.take();
            // 再从 requestTable 取出映射对象, 这边可能是为了做校验和去重.
            AllocateRequest expectedRequest = this.requestTable.get(req.getFilePath());
            if (null == expectedRequest) {
                log.warn("this mmap request expired, maybe cause timeout " + req.getFilePath() + " " + req.getFileSize());
                return true;
            }
            if (expectedRequest != req) {
                log.warn("never expected here,  maybe cause timeout " + req.getFilePath() + " " + req.getFileSize() + ", req:" +
                        req + ", expectedRequest:" + expectedRequest);
                return true;
            }
            // 如果这个请求的 MappedFile 已经创建好了, 那么方法直接返回true, 也不需要创建了.
            if (req.getMappedFile() == null) {
                long beginTime = System.currentTimeMillis();
                MappedFile mappedFile;
                // isTransientStorePoolEnable()为true有3个条件：开启缓冲池功能 + 异步刷盘 + master broker.
                if (messageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                    try {
                        // rocketMQ 这边支持 java spi 方式初始化自定义的 MappedFile 扩展类
                        mappedFile = ServiceLoader.load(MappedFile.class).iterator().next();
                        // 直接调用init()方法初始化 MappedFile 对象.
                        mappedFile.init(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
                    } catch (RuntimeException e) {
                        // 如果失败了, 那么使用默认的实现类
                        log.warn("Use default implementation.");
                        mappedFile = new MappedFile(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
                    }
                } else {
                    // isTransientStorePoolEnable()返回false, 直接使用默认的 MappedFile 实现.
                    mappedFile = new MappedFile(req.getFilePath(), req.getFileSize());
                }

                // 记录耗时时间.
                long elapsedTime = UtilAll.computeElapsedTimeMilliseconds(beginTime);
                if (elapsedTime > 10) {
                    int queueSize = this.requestQueue.size();
                    log.warn("create mappedFile spent time(ms) " + elapsedTime + " queue size " + queueSize + " " + req.getFilePath() + " " + req.getFileSize());
                }

                // 预写 MappedFile 文件.
                // 但是这些限制了, 只会预写文件大小大于1G的, 那一般情况就是 commit log 文件.
                if (mappedFile.getFileSize() >= this.messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog() && this.messageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
                    // 开始刷入文件, 默认写入的都是0
                    mappedFile.warmMappedFile(this.messageStore.getMessageStoreConfig().getFlushDiskType(), this.messageStore.getMessageStoreConfig().getFlushLeastPagesWhenWarmMapedFile());
                }
                // 将创建好的 MappedFile 保存, 同时设置一些标识位.
                req.setMappedFile(mappedFile);
                this.hasException = false;
                isSuccess = true;
            }
        } catch (InterruptedException e) {
            // 被中断异常, 会终止这个类的 run() 运行.
            log.warn(this.getServiceName() + " interrupted, possibly by shutdown.");
            this.hasException = true;
            return false;
        } catch (IOException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
            this.hasException = true;
            // 出现 I/O 异常, 将请求重新打回优先级队列中.
            if (null != req) {
                requestQueue.offer(req);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                }
            }
        } finally {
            // 这边执行唤醒的原因, 在于：putRequestAndReturnMappedFile() 方法最后面会阻塞等待 MappedFile 文件创建好
            if (req != null && isSuccess) req.getCountDownLatch().countDown();
        }
        return true;
    }

    /**
     * 初始化磁盘文件的请求.
     */
    static class AllocateRequest implements Comparable<AllocateRequest> {
        // Full file path
        private String filePath;
        private int fileSize;
        private CountDownLatch countDownLatch = new CountDownLatch(1);
        private volatile MappedFile mappedFile = null;

        public AllocateRequest(String filePath, int fileSize) {
            this.filePath = filePath;
            this.fileSize = fileSize;
        }

        public String getFilePath() {
            return filePath;
        }

        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }

        public int getFileSize() {
            return fileSize;
        }

        public void setFileSize(int fileSize) {
            this.fileSize = fileSize;
        }

        public CountDownLatch getCountDownLatch() {
            return countDownLatch;
        }

        public void setCountDownLatch(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        public MappedFile getMappedFile() {
            return mappedFile;
        }

        public void setMappedFile(MappedFile mappedFile) {
            this.mappedFile = mappedFile;
        }

        public int compareTo(AllocateRequest other) {
            if (this.fileSize < other.fileSize) return 1;
            else if (this.fileSize > other.fileSize) {
                return -1;
            } else {
                int mIndex = this.filePath.lastIndexOf(File.separator);
                long mName = Long.parseLong(this.filePath.substring(mIndex + 1));
                int oIndex = other.filePath.lastIndexOf(File.separator);
                long oName = Long.parseLong(other.filePath.substring(oIndex + 1));
                if (mName < oName) {
                    return -1;
                } else if (mName > oName) {
                    return 1;
                } else {
                    return 0;
                }
            }
            // return this.fileSize < other.fileSize ? 1 : this.fileSize >
            // other.fileSize ? -1 : 0;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((filePath == null) ? 0 : filePath.hashCode());
            result = prime * result + fileSize;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            AllocateRequest other = (AllocateRequest) obj;
            if (filePath == null) {
                if (other.filePath != null) return false;
            } else if (!filePath.equals(other.filePath)) return false;
            if (fileSize != other.fileSize) return false;
            return true;
        }
    }
}
