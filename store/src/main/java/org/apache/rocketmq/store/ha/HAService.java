package org.apache.rocketmq.store.ha;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.PutMessageStatus;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * HA主从同步类
 */
public class HAService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 已经创建并连接好的{@link HAConnection}的数量统计
     */
    private final AtomicInteger connectionCount = new AtomicInteger(0);

    /**
     * 与slave的连接
     */
    private final List<HAConnection> connectionList = new LinkedList<>();

    /**
     * 用来接受slave的请求, 为其建立一个连接{@link HAConnection}
     */
    private final AcceptSocketService acceptSocketService;

    /**
     * 消息存储引擎
     */
    private final DefaultMessageStore defaultMessageStore;

    /**
     * 用于阻塞唤醒
     */
    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();

    /**
     * 已同步给slave的最大commitlog偏移量
     */
    private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    /**
     * 接收主从同步的请求, 用于给slave同步数据
     */
    private final GroupTransferService groupTransferService;

    /**
     * 给slave使用, 是HA主从同步的客户端
     */
    private final HAClient haClient;

    public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        this.acceptSocketService = new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
        this.groupTransferService = new GroupTransferService();
        this.haClient = new HAClient();
    }

    /**
     * 更新master的地址
     *
     * @param newAddr 新地址
     */
    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    /**
     * 提交HA同步请求
     *
     * @param request 请求数据
     */
    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    /**
     * 判断slave是否可用, 两个条件：
     * 1、至少存在一条salve连接;
     * 2、至少一个slave与master同步的commitlog数据偏差小于256M;
     *
     * @param masterPutWhere master落地commitlog的偏移量
     * @return true-slave可用
     */
    public boolean isSlaveOK(final long masterPutWhere) {
        // 至少存在一个slave连接
        boolean result = this.connectionCount.get() > 0;
        // 至少一个slave与master同步的commitlog数据偏移量, 偏差小于默认值, 即256M.
        result = result && ((masterPutWhere - this.push2SlaveMaxOffset.get()) < this.defaultMessageStore.getMessageStoreConfig().getHaSlaveFallbehindMax());
        return result;
    }

    /**
     * 唤醒{@link GroupTransferService}
     *
     * @param offset slave响应给master, 它已经更新好的commitlog偏移量
     */
    public void notifyTransferSome(final long offset) {
        // 循环条件, HAService维护的push2SlaveMaxOffset变量值小于slave已确认的
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            // CAS更新变量值为salve返回的偏移量
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            // 更新成功就执行唤醒, 唤醒的是org.apache.rocketmq.store.ha.HAService.GroupTransferService线程
            // 更新失败就重复更新
            if (ok) {
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    /**
     * 当前master有多少个slave与其建立连接
     *
     * @return slave数量
     */
    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    public void start() throws Exception {
        // 初始化好的nio的Selector和ServerSocketChannel
        this.acceptSocketService.beginAccept();
        // 依次启动各个子服务
        this.acceptSocketService.start();
        this.groupTransferService.start();
        this.haClient.start();
    }

    public void shutdown() {
        this.haClient.shutdown();
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
    }

    /**
     * 线程安全地, 添加slave连接
     */
    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    /**
     * 线程安全地, 移除slave连接
     */
    public void removeConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    /**
     * 线程安全地, 销毁连接
     */
    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }
            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    /**
     * 接收slave的连接, 并为其创建{@link HAConnection}
     */
    class AcceptSocketService extends ServiceThread {
        // 监听地址
        private final SocketAddress socketAddressListen;
        // 监听通道
        private ServerSocketChannel serverSocketChannel;
        // 选择器
        private Selector selector;

        public AcceptSocketService(final int port) {
            // 默认接收任何主机地址的连接请求, 默认监听端口号为：10912
            this.socketAddressListen = new InetSocketAddress(port);
        }

        /**
         * Starts listening to slave connections.
         *
         * @throws Exception If fails.
         */
        public void beginAccept() throws Exception {
            // 打开服务通道
            this.serverSocketChannel = ServerSocketChannel.open();
            // 创建选择器
            this.selector = RemotingUtil.openSelector();
            // 判断服务通道, 设置tcp参数
            this.serverSocketChannel.socket().setReuseAddress(true);
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            this.serverSocketChannel.configureBlocking(false);
            // 将其注册到选择器上, 实现I/O多路复用
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                this.serverSocketChannel.close();
                this.selector.close();
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                try {
                    // 阻塞等待1s, 看看有没有slave连接
                    this.selector.select(1000);
                    // 获取I/O事件, 如果是超时唤醒, 那么这边会返回一个空集合
                    Set<SelectionKey> selected = this.selector.selectedKeys();
                    // 处理I/O事件
                    if (selected != null) {
                        for (SelectionKey k : selected) {
                            // 拿I/O事件的ops标识, 与接收请求事件的ops标识进行位运算,
                            // 如果值不为1, 说明存在接收请求的事件, 就会处理这个事件.
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                                // 这边就是nio的知识了, 通过选择键获取关联的通道, 这里就是：ServerSocketChannel
                                // 调用它的accept()方法获取到对端的通道
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();
                                if (sc != null) {
                                    HAService.log.info("HAService receive new connection, " + sc.socket().getRemoteSocketAddress());
                                    try {
                                        // 为其创建一个新的连接
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        // 启动这个连接, 其实就是启动读写两个线程
                                        conn.start();
                                        // 将连接存储起来
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }
                        // nio要求处理完事件需要手动清理, 不然下次还会拿到这些事件, 那是调用选择键方法时就会出现npe异常.
                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }
            log.info(this.getServiceName() + " service end");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * 用来处理HA同步请求.
     * 实现思想跟{@link CommitLog.GroupCommitService}一样
     */
    class GroupTransferService extends ServiceThread {

        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();

        /**
         * 定义读写集合, 一个用于添加HA同步请求, 一个用于读取HA同步请求.
         * 主要思想就是读写分离, 减少锁冲突, 每次处理请求时都会直接将两个请求集合互换.
         */
        private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new ArrayList<>();
        private volatile List<CommitLog.GroupCommitRequest> requestsRead = new ArrayList<>();

        /**
         * 提供给{@link HAService}调用, 接收HA同步请求
         *
         * @param request 数据同步请求
         */
        public synchronized void putRequest(final CommitLog.GroupCommitRequest request) {
            // 线程安全地, 将请求添加到写集合中
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            // 唤醒这个线程, 外部多次调用, 只会唤醒一次.
            this.wakeup();
        }

        /**
         * {@link GroupTransferService}会一直处理主从同步请求, 当请求还未同步成功, 它会阻塞一段时间.
         * 当然, 它既可以超时被唤醒返回, 也可以通过这个方法被唤醒.
         */
        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }

        /**
         * 交换读写两集合
         */
        private void swapRequests() {
            List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        /**
         * 处理主从同步请求, 但不是实际发送数据的线程, 实际传输数据在{@link HAConnection.WriteSocketService}
         */
        private void doWaitTransfer() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    // 处理每一个HA同步请求
                    for (CommitLog.GroupCommitRequest req : this.requestsRead) {

                        // 当前master同步给slave的最大偏移量  大于等于  master落地的commitlog刷盘点, 那就表示传输给slave成功了.
                        // 如果小于, 那就说明数据还没同步给slave.
                        boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();

                        // 算出最大等待时间点, 默认是 now + 同步刷盘的最大超时时间
                        long waitUntilWhen = HAService.this.defaultMessageStore.getSystemClock().now() + HAService.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout();

                        // 循环条件：
                        // 1、slave一直未同步当前master落地commitlog的刷盘偏移量;
                        // 2、未到达最大超时等待时间点;
                        while (!transferOK && HAService.this.defaultMessageStore.getSystemClock().now() < waitUntilWhen) {
                            // 那么执行GroupTransferService服务的线程就等待1s
                            this.notifyTransferObject.waitForRunning(1000);
                            // 被唤醒或者1s时间到以后, 再次判断主从是否同步成功
                            transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        }

                        // 跳出循环, 但是同步失败, 说明是因为超时跳出循环的
                        if (!transferOK) {
                            log.warn("transfer messsage to slave timeout, " + req.getNextOffset());
                        }

                        // 不管是成功主从同步, 还是超时主从同步, 都将结果设置回主从同步请求, 以便外部线程及时响应.
                        req.wakeupCustomer(transferOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                    }

                    // 清空读集合
                    this.requestsRead.clear();
                }
            }
        }

        public void run() {
            log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                try {
                    // 等待唤醒或者, 10m后超时唤醒
                    this.waitForRunning(10);
                    // 执行请求, 只有在被唤醒的情况下, 读写两集合才会互换, 超时唤醒是没有互换的.
                    this.doWaitTransfer();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }
            log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            // 每次唤醒后, 都会交换读写两集合
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupTransferService.class.getSimpleName();
        }
    }

    /**
     * HA客户端
     */
    class HAClient extends ServiceThread {

        /**
         * 最大读缓存大小, 4MB
         */
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;

        /**
         * master的IP地址
         */
        private final AtomicReference<String> masterAddress = new AtomicReference<>();

        /**
         * 存储上报的偏移量的缓冲区
         */
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);

        /**
         * 与master交互的底层TCP通道
         */
        private SocketChannel socketChannel;

        /**
         * I/O多路复用
         */
        private Selector selector;

        /**
         * 最后一次上报的时间
         */
        private long lastWriteTimestamp = System.currentTimeMillis();

        /**
         * 上报给master的commitlog偏移量, slave启动时, 等于它自己机器上的最大的commitlog偏移量
         */
        private long currentReportedOffset = 0;

        /**
         * 记录每次处理master发来的数据时, 当前已处理的buffer position
         */
        private int dispatchPosition = 0;

        /**
         * 用来接收master同步过来的commitlog数据
         */
        private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        public HAClient() throws IOException {
            // 获取选择器
            this.selector = RemotingUtil.openSelector();
        }

        /**
         * 更新master的IP地址
         *
         * @param newAddr 新地址
         */
        public void updateMasterAddress(final String newAddr) {
            String currentAddr = this.masterAddress.get();
            if (currentAddr == null || !currentAddr.equals(newAddr)) {
                this.masterAddress.set(newAddr);
                log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                try {
                    // 连接上master才会处理
                    if (this.connectMaster()) {

                        // 到点了, 需要跟master上报一下commitlog偏移量(一般作为心跳)
                        if (this.isTimeToReportOffset()) {
                            // 如果上报失败, 会关闭与master的连接
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                            if (!result) {
                                this.closeMaster();
                            }
                        }

                        // 阻塞1s, 等待I/O事件
                        this.selector.select(1000);

                        // 处理master发过来的commitlog数据, 更新失败同样会关闭与master的连接
                        boolean ok = this.processReadEvent();
                        if (!ok) {
                            this.closeMaster();
                        }

                        // 更新完master同步过来的commitlog数据, 如果最大偏移量跟上报给master的偏移量有出入,
                        // 会再重新上报一次.
                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }

                        // 连接空闲超时判断
                        long interval = HAService.this.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                            log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress + "] expired, " + interval);
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }
                    } else {
                        // 连接master失败,
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    this.waitForRunning(1000 * 5);
                }
            }
            log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return HAClient.class.getSimpleName();
        }

        /**
         * 判断是否需要向master上报已同步的偏移量
         * 其实就是判断距离上一次发送数据的时间差是否超过了心跳包的间隔时间.
         *
         * @return true-需要发送心跳包
         */
        private boolean isTimeToReportOffset() {
            long interval = HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
            return interval > HAService.this.defaultMessageStore.getMessageStoreConfig().getHaSendHeartbeatInterval();
        }

        /**
         * 上报slave当前已同步的最大的commitlog偏移量
         *
         * @param maxOffset 最大偏移量
         * @return true-同步成功
         */
        private boolean reportSlaveMaxOffset(final long maxOffset) {
            // 这三行代码为了设值
            this.reportOffset.position(0);
            this.reportOffset.limit(8);
            this.reportOffset.putLong(maxOffset);
            // 这两行代码为了读值
            this.reportOffset.position(0);
            this.reportOffset.limit(8);

            // 往底层通道写入数据, 传输给master. 担心网络拥堵, 所以rocketMQ重试了三次,
            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
                try {
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error(this.getServiceName() + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }
            // 更新上报时间
            lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
            // 缓冲区数据没剩余, 就表示上报成功.
            return !this.reportOffset.hasRemaining();
        }

        /**
         * 这个方法只有在{@link #byteBufferRead}没有剩余数据可以读取了才会调用.
         * 用来重新分配缓冲区使用.
         */
        private void reallocateByteBuffer() {
            // 备份还没处理的数据
            int remain = READ_MAX_BUFFER_SIZE - this.dispatchPosition;
            if (remain > 0) {
                this.byteBufferRead.position(this.dispatchPosition);
                // 将其存储到backup中
                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
                this.byteBufferBackup.put(this.byteBufferRead);
            }

            // 交换两个缓冲区
            this.swapByteBuffer();

            // 重置读缓冲区的大小
            this.byteBufferRead.position(remain);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);

            // 重置为0
            this.dispatchPosition = 0;
        }

        private void swapByteBuffer() {
            ByteBuffer tmp = this.byteBufferRead;
            this.byteBufferRead = this.byteBufferBackup;
            this.byteBufferBackup = tmp;
        }

        /**
         * 处理master同步的commitlog数据
         *
         * @return true-处理成功
         */
        private boolean processReadEvent() {
            // 空读次数
            int readSizeZeroTimes = 0;
            // 循环条件：缓冲区还有数据可以读取
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    // 从TCP通道中拿数据, 当然如果master没同步数据, 此次拿到的数据就为0
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    // 如果master有数据返回
                    if (readSize > 0) {
                        readSizeZeroTimes = 0;
                        // 数据处理
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                    } else if (readSize == 0) {
                        // 空读次数累加1, 达到三次以后, 退出循环, 方法返回true
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        // read()返回值为-1, 说明对端的数据发送完毕, 并且主动的close socket.
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }
            return true;
        }

        /**
         * 实际处理master同步的commitlog数据
         *
         * @return true-处理成功
         */
        private boolean dispatchReadRequest() {

            // 理解这边需要先分析 HAConnection.WriteSocketService 类, 它每次同步会单独发送一个header, 再发送一个body(TCP是严格有序的)
            // 其中header包括两个数据, 一个是当前master与slave同步的最新commitlog偏移量, 一个就是commitlog数据大小.
            final int msgHeaderSize = 8 + 4;

            // 这边的position是针对写入, 即需要执行一次flip()才能读取里面的数据.
            // 处理数据之前, 先将起始值保存起来
            int readSocketPos = this.byteBufferRead.position();

            while (true) {
                // 当前readBuffer的写入位置 - 上一次处理的位置 = 数据量
                int diff = this.byteBufferRead.position() - this.dispatchPosition;

                // 数据量大于等于header的大小, 读取header
                if (diff >= msgHeaderSize) {

                    // 读取header数据
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);

                    // slave当前的最大commitlog偏移量
                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                    // 当前slave最大的commitlog偏移量与master同步的commitlog偏移量不一致,
                    // 那说明一定有网络数据包丢失, 导致部分commitlog数据没被更新到slave.
                    if (slavePhyOffset != 0) {
                        if (slavePhyOffset != masterPhyOffset) {
                            log.error("master pushed offset not equal the max phy offset in slave, SLAVE: " + slavePhyOffset + " MASTER: " + masterPhyOffset);
                            return false;
                        }
                    }

                    // 此次传输大于等于 header+body 大小, 开始解析commitlog数据
                    if (diff >= (msgHeaderSize + bodySize)) {
                        byte[] bodyData = new byte[bodySize];

                        // 获取commitlog数据
                        this.byteBufferRead.position(this.dispatchPosition + msgHeaderSize);
                        this.byteBufferRead.get(bodyData);

                        // 将数据追加到slave的commitlog中
                        HAService.this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData);

                        // 还原readBuffer之前的position位置
                        this.byteBufferRead.position(readSocketPos);

                        // 更新已处理的位置
                        this.dispatchPosition += msgHeaderSize + bodySize;

                        // 判断是否需要再上报master
                        if (!reportSlaveMaxOffsetPlus()) {
                            return false;
                        }

                        // 继续处理, 有可能master发了多笔数据过来
                        continue;
                    }
                }

                // 处理完缓冲区所有数据, 重置它
                if (!this.byteBufferRead.hasRemaining()) {
                    this.reallocateByteBuffer();
                }

                break;
            }//while end

            return true;
        }

        /**
         * 判断slave当前最大的commitlog偏移量, 与上报给master的偏移量, 是否存在偏差,
         * 如果是, 会重新上报一次
         *
         * @return true-上报成功
         */
        private boolean reportSlaveMaxOffsetPlus() {
            boolean result = true;
            long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
            if (currentPhyOffset > this.currentReportedOffset) {
                this.currentReportedOffset = currentPhyOffset;
                result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                if (!result) {
                    this.closeMaster();
                    log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
                }
            }
            return result;
        }

        /**
         * 连接master, 判断的依据就是{@link #socketChannel}是否不为空
         *
         * @return true-已连接
         */
        private boolean connectMaster() throws ClosedChannelException {
            // 如果socketChannel为空, 说明还未连接master
            if (null == socketChannel) {
                // 获取master的IP地址
                String addr = this.masterAddress.get();
                if (addr != null) {
                    // 转换出master的IP地址
                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {
                        // 连接master, 并且注册读事件
                        this.socketChannel = RemotingUtil.connect(socketAddress);
                        if (this.socketChannel != null) {
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }
                // slave启动, 获取自己机器上当前最大的commitlog偏移量, 作为需要同步给master的偏移量
                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
                // 启动的时间就作为最后一次更新的时间点
                this.lastWriteTimestamp = System.currentTimeMillis();
            }
            return this.socketChannel != null;
        }

        /**
         * 关闭与master的连接
         */
        private void closeMaster() {
            if (null != this.socketChannel) {
                try {
                    // 取消I/O事件
                    SelectionKey sk = this.socketChannel.keyFor(this.selector);
                    if (sk != null) {
                        sk.cancel();
                    }
                    // 关闭通道
                    this.socketChannel.close();
                    this.socketChannel = null;
                } catch (IOException e) {
                    log.warn("closeMaster exception. ", e);
                }

                // 变量值还原
                this.lastWriteTimestamp = 0;
                this.dispatchPosition = 0;

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

                this.byteBufferRead.position(0);
                this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            }
        }
    }
}
