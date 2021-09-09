package org.apache.rocketmq.store.ha;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * HA主从同步, 底层TCP连接的封装, 这个类可能现在没在用, 改用了{@link DLedgerCommitLog}实现高可用, 但这个类是学习主从同步的重要代码实践.
 * <p>
 * 这个类主要是给 Master 使用, 当{@link HAService.HAClient}发起连接连接后, {@link HAService}
 * 就会收到连接, 并为其创建一个{@link HAConnection}.
 * <p>
 * 这个类有两个内部类：
 * 1.{@link ReadSocketService}, 它是用来读取{@link HAService.HAClient}上报的已落地的commitlog偏移量
 * 2.{@link WriteSocketService}, 用来给{@link HAService.HAClient}发送commitlog数据.
 */
public class HAConnection {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * {@link HAService}的引用
     */
    private final HAService haService;

    /**
     * master 跟 slave 的TCP底层通道
     */
    private final SocketChannel socketChannel;

    /**
     * {@link HAService.HAClient}的IP地址
     */
    private final String clientAddr;

    /**
     * 这两个service都是基于{@link #socketChannel}创建的, 它们分别处理这个通道的写事件和读事件.
     * 其中, 读事件：{@link HAService.HAClient}上报的已更新的commitlog偏移量.
     * 写事件：master同步给slave数据(commitlog消息).
     */
    private WriteSocketService writeSocketService;
    private ReadSocketService readSocketService;

    /**
     * salve连接上master, 第一次上报, 需要同步的偏移量, 即slave告诉master, 我要这个位置的commitlog消息.
     * 这个变量只会在第一次上报的时候更新, 它记录slave最开始需要的commitlog偏移量
     */
    private volatile long slaveRequestOffset = -1;

    /**
     * slave已经响应的偏移量(即slave落地的最大commitlog偏移量)
     */
    private volatile long slaveAckOffset = -1;

    public HAConnection(final HAService haService, final SocketChannel socketChannel) throws IOException {
        this.haService = haService;
        this.socketChannel = socketChannel;
        this.clientAddr = this.socketChannel.socket().getRemoteSocketAddress().toString();

        // socketChannel的配置:
        this.socketChannel.configureBlocking(false);                 //非阻塞模式
        this.socketChannel.socket().setSoLinger(false, -1);//
        this.socketChannel.socket().setTcpNoDelay(true);             //禁用了Nagle算法
        this.socketChannel.socket().setReceiveBufferSize(1024 * 64); //接收缓冲区大小
        this.socketChannel.socket().setSendBufferSize(1024 * 64);    //发送缓冲区大小

        // 读写两个服务共用一个 socketChanel, 其实就是将读写事件分离开
        this.writeSocketService = new WriteSocketService(this.socketChannel);
        this.readSocketService = new ReadSocketService(this.socketChannel);

        // 连接数加1
        this.haService.getConnectionCount().incrementAndGet();
    }

    public void start() {
        // 就是启动读写两个类
        this.readSocketService.start();
        this.writeSocketService.start();
    }

    public void shutdown() {
        // 关闭两个读写线程, 然后关闭底层通道
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.close();
    }

    public void close() {
        // 关闭底层通道
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }
        }
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    /**
     * 处理底层通道的读事件, 用来更新 slave 即{@link HAService.HAClient}, 上报的已更新的commitlog偏移量.
     */
    class ReadSocketService extends ServiceThread {

        // 用来指定读缓冲区的大小, 默认1KB
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;

        private final Selector selector;
        private final SocketChannel socketChannel;

        // 用来存储对端发送的数据, 限制大小为1KB, 可以多次重复使用.
        // 当已经读取到的数据达到1KB以后, 就会重置这个缓冲区, 即 position=0, limit=1024*1024
        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        // 用来记录已经处理的 byteBufferRead 偏移量, 一直被累加, 直至 byteBufferRead 读满, 然后被重置为0
        private int processPosition = 0;

        // 上一次读取的时间点
        private volatile long lastReadTimestamp = System.currentTimeMillis();

        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            // rocketMQ使用多路复用的方式来处理HA读写事件
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            // 注册读事件
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            // 设置为后台线程
            this.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 默认等待处理1s
                    this.selector.select(1000);
                    // 核心方法, 处理对端发来的数据
                    boolean ok = this.processReadEvent();
                    if (!ok) {
                        // 处理失败, 跳出循环
                        HAConnection.log.error("processReadEvent error");
                        break;
                    }
                    // 处理成功, 计算距离上一次处理的间隔时间, 即 master 与 slave 没有数据交互的时间间隔
                    long interval = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;
                    // 间隔时间大于配置值, 说明间隔太久了, 跳出循环, 停止这条连接
                    if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                        log.warn("ha housekeeping, found this connection[" + HAConnection.this.clientAddr + "] expired, " + interval);
                        break;
                    }
                } catch (Exception e) {
                    // 出现异常也跳出循环
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            /*
             * 代码走到这里, 意味着这条HA连接（HAConnection）要被断开回收
             */

            // 将线程状态标识为停止状态
            this.makeStop();

            // 将对应的写线程也标识为中止状态
            writeSocketService.makeStop();

            // 向 HAService 移除这条连接
            haService.removeConnection(HAConnection.this);

            // 向 HAService 减少ha连接数
            HAConnection.this.haService.getConnectionCount().decrementAndGet();

            // 取消底层TCP通道注册在选择键上的事件, 即Read事件
            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            // 关闭资源
            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReadSocketService.class.getSimpleName();
        }

        /**
         * 读线程的核心方法
         *
         * @return true-处理成功
         */
        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;

            // byteBufferRead是可以在多次网络I/O中重复使用的, 所以它的position属性是一直在累加.
            // 当其达到limit时, 此时hasRemaining()方法就会返回false, 进入if语句块, 将读缓冲区重置,
            // 并重置processPosition为0.
            if (!this.byteBufferRead.hasRemaining()) {
                this.byteBufferRead.flip();
                this.processPosition = 0;
            }

            // 循环条件：读缓冲区没有填满
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    // 尝试从对端读取数据, 如果对端有数据发送, 就会添加到读缓冲区byteBufferRead, 缓冲区的position属性就会被累加,
                    // 同时返回读取的字节数. 当然如果slave未发送数据, 那么调用此方法就会返回0
                    int readSize = this.socketChannel.read(this.byteBufferRead);

                    // 读到数据
                    if (readSize > 0) {
                        // 记录空读的次数
                        readSizeZeroTimes = 0;
                        // 更新读取时间点
                        this.lastReadTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();

                        // HAClient会上报它接收到的commit log offset, 是一个long类型的数据, 因此字节数为8
                        // 这个if条件的要求, 就是保证读取到的字节数至少为8(因为TCP是面向流的网络协议, 有可能发生粘包, 传递了超过8个字节的数据过来)
                        if ((this.byteBufferRead.position() - this.processPosition) >= 8) {

                            // 保证pos在 byteBufferRead 的位置是8的整数倍
                            int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % 8);

                            // pos - 8 得到一个long数据在 byteBufferRead 的起始位置, 相当于将 position 左移8位, 再从这边开始读取8个字节, 解码成long数据.
                            // 这么处理的原因是：有可能slave会传输多个offset给master, 而master只读取最末尾的那个offset, 即一次请求中最大的偏移量.
                            // 注意这种方式-java.nio.ByteBuffer.getLong(int), 是不会更新 position 位置的.
                            long readOffset = this.byteBufferRead.getLong(pos - 8);

                            // 更新 processPosition 的位置, 已经读取到pos这个位置
                            this.processPosition = pos;

                            // 更新偏移量, 这个 readOffset 就是 slave 返回的已经存储到它所在broker的commitlog的偏移量.
                            // slave在这块代码发出请求：org.apache.rocketmq.store.ha.HAService.HAClient.reportSlaveMaxOffset()
                            HAConnection.this.slaveAckOffset = readOffset;

                            // 初始化请求偏移量, 一开始等于第一次从slave读取到的偏移量
                            if (HAConnection.this.slaveRequestOffset < 0) {
                                HAConnection.this.slaveRequestOffset = readOffset;
                                log.info("slave[" + HAConnection.this.clientAddr + "] request offset " + readOffset);
                            }

                            // 当处理完slave上报的消息时, 说明slave已经将master发给它的commitlog数据处理好了.
                            // 此时就会唤醒阻塞的线程, 即唤醒：org.apache.rocketmq.store.ha.HAService.GroupTransferService.
                            // 通知它, 此次主从同步成功.
                            HAConnection.this.haService.notifyTransferSome(HAConnection.this.slaveAckOffset);
                        }
                    } else if (readSize == 0) {
                        // 不存在数据, 最大只允许读取3次, 3次过后退出while循环, 方法返回true, 然后继续休眠
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        // read()返回值为-1, 说明对端的数据发送完毕, 并且主动的close socket.
                        // 此时返回false, 表示TCP连接断开, 读取失败
                        log.error("read socket[" + HAConnection.this.clientAddr + "] < 0");
                        return false;
                    }
                } catch (IOException e) {
                    // 异常情况, 返回false
                    log.error("processReadEvent exception", e);
                    return false;
                }
            }
            // 其它情况返回true
            return true;
        }
    }

    /**
     * 处理底层通道的写事件, 用来给 slave 发送commit log消息内容
     */
    class WriteSocketService extends ServiceThread {
        private final Selector selector;
        private final SocketChannel socketChannel;

        // header的数据包大小, 8个字节的commitlog offset, 4字节的commitlog数据大小
        private final int headerSize = 8 + 4;

        // 用来传输header使用
        private final ByteBuffer byteBufferHeader = ByteBuffer.allocate(headerSize);

        // 用于构建header, 写回slave
        private long nextTransferFromWhere = -1;

        // 用来传输body使用
        private SelectMappedBufferResult selectMappedBufferResult;

        // 标记上一次传输是否已经完成, 因为commitlog消息体比较大, 会分为多次传输.
        // 每传输成功一次, 这个变量就会置为true. 这个变量的值等于 transferData()返回值
        private boolean lastWriteOver = true;

        // header 或 body 写出的时间点
        private long lastWriteTimestamp = System.currentTimeMillis();

        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);

                    // slave未上报过偏移量(可能是第一次启动, 也可能是宕机后重启), 这种情况下master拿不到slave当前的commitlog偏移量,
                    // 就不知道要从哪里开始讲数据同步给slave, 所以只能继续选择休眠
                    if (-1 == HAConnection.this.slaveRequestOffset) {
                        Thread.sleep(10);
                        continue;
                    }

                    // 第一次传输数据给slave, 两种情况：
                    // 1. slave第一次启动;
                    // 2. slave宕机启动;
                    if (-1 == this.nextTransferFromWhere) {

                        // 等于0意味着slave第一次请求同步master(即slave所在的broker没有任何commitlog数据)
                        if (0 == HAConnection.this.slaveRequestOffset) {
                            // master目前存储的commitlog消息最大偏移量(物理偏移量, 算入了 MappedFile#fileFromOffset)
                            long masterOffset = HAConnection.this.haService.getDefaultMessageStore().getCommitLog().getMaxOffset();
                            // 这步就是获取 MappedFile#fileFromOffset
                            masterOffset = masterOffset - (masterOffset % HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getMappedFileSizeCommitLog());

                            if (masterOffset < 0) {
                                masterOffset = 0;
                            }
                            // 头次传输数据, 就从master当前使用的最后一个commitlog文件开始传输
                            this.nextTransferFromWhere = masterOffset;
                        } else {
                            // 不等于0, 说明是slave之前已经同步过数据, 此时会使用第一次正常同步使用的偏移量开始同步
                            this.nextTransferFromWhere = HAConnection.this.slaveRequestOffset;
                        }
                        log.info("master transfer data from " + this.nextTransferFromWhere + " to slave[" + HAConnection.this.clientAddr + "], and slave request " + HAConnection.this.slaveRequestOffset);
                    }

                    // 如果上一次传输已经完成
                    if (this.lastWriteOver) {

                        // master和slave两次间隔的心跳时间
                        long interval = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;

                        // master和slave最大空闲时间为5s, 如果距离上一次传输已经超过5s, 就是5s内没有任何数据交互, master会选择发送一个空的数据包给slave.
                        if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaSendHeartbeatInterval()) {
                            // Build Header
                            this.byteBufferHeader.position(0);
                            this.byteBufferHeader.limit(headerSize);
                            this.byteBufferHeader.putLong(this.nextTransferFromWhere);
                            this.byteBufferHeader.putInt(0);
                            this.byteBufferHeader.flip();
                            // 执行传输一次空数据包
                            this.lastWriteOver = this.transferData();
                            if (!this.lastWriteOver) continue;
                        }
                    } else {
                        // 上一次传输还没有完成, 直接调用方法, 继续传输
                        this.lastWriteOver = this.transferData();
                        if (!this.lastWriteOver) continue;
                    }

                    /*
                     * 代码到这里, 上一次已经传输完成, 并且master与slave之间未发生心跳超时
                     */

                    // 在master中读取指定偏移量的commitlog数据
                    SelectMappedBufferResult selectResult = HAConnection.this.haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);
                    if (selectResult != null) {
                        // 一次主从传输最多32k的数据
                        int size = selectResult.getSize();
                        if (size > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
                            size = HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                        }

                        // 此次更新的commitlog起始位置
                        long thisOffset = this.nextTransferFromWhere;

                        // 更新下次同步的commitlog起始位置
                        this.nextTransferFromWhere += size;

                        // 设置此次消息体的最大可读位置, 然后赋值给变量 selectMappedBufferResult.
                        selectResult.getByteBuffer().limit(size);

                        // 每次传输完(即调用transferData()), 会将变量置为null, 然后等待下次传输重新赋值.
                        this.selectMappedBufferResult = selectResult;

                        // Build Header
                        this.byteBufferHeader.position(0);
                        this.byteBufferHeader.limit(headerSize);
                        // 设置此次同步的commitlog偏移量、设置同步的commitlog数据大小
                        this.byteBufferHeader.putLong(thisOffset);
                        this.byteBufferHeader.putInt(size);
                        this.byteBufferHeader.flip();

                        this.lastWriteOver = this.transferData();
                    } else {
                        // 读取不到新的commitlog消息, 阻塞等待被唤醒, 或者超时唤醒
                        HAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
                    }
                } catch (Exception e) {
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            /*
             * 跳出while循环, 说明线程被停止了
             */

            // 将当前线程移出 HAService 维护的阻塞table
            HAConnection.this.haService.getWaitNotifyObject().removeFromWaitingThreadTable();

            // 释放mappedResult
            if (this.selectMappedBufferResult != null) {
                this.selectMappedBufferResult.release();
            }

            // 此线程标识为停止
            this.makeStop();

            // 读线程标识为停止
            readSocketService.makeStop();

            // 移除本条HA连接
            haService.removeConnection(HAConnection.this);

            // 取消选择键
            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            // 释放资源
            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        /**
         * 传输数据给salve
         *
         * @return true-有数据写出
         * @throws Exception 异常
         */
        private boolean transferData() throws Exception {
            // 空写次数记录
            int writeSizeZeroTimes = 0;
            // Write Header
            while (this.byteBufferHeader.hasRemaining()) {
                // 将数据写回到通道中, 传输给slave
                int writeSize = this.socketChannel.write(this.byteBufferHeader);
                if (writeSize > 0) {
                    // 实际有数据写出, 空写次数记录重置为0, 记录本次写出的时间点.
                    writeSizeZeroTimes = 0;
                    this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                } else if (writeSize == 0) {
                    // 实际没有数据写出, 空写次数记录累加1
                    if (++writeSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    // 其它异常情况
                    throw new Exception("ha master write header error < 0");
                }
            }// while循环结束, 要么header已经写完了, 要么达到空写最大限制

            // body没有数据可以写, 返回header缓冲区是否还有剩余
            // (有可能网络拥堵, 或者对端接收缓冲区已满, 因此数据写不出去, 就会存在剩余的情况)
            if (null == this.selectMappedBufferResult) {
                return !this.byteBufferHeader.hasRemaining();
            }

            /*
             * 代码走到这边, 意味着body有数据可以写
             */

            // 重置空写次数为0
            writeSizeZeroTimes = 0;

            // Write Body
            if (!this.byteBufferHeader.hasRemaining()) {
                while (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                    // 将commitlog数据写到通道中
                    int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());
                    if (writeSize > 0) {
                        // 实际有消息写出, 记录写出时间点
                        writeSizeZeroTimes = 0;
                        this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                    } else if (writeSize == 0) {
                        // 实际没有消息写出, 空写次数记录累加1
                        if (++writeSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        // 异常情况
                        throw new Exception("ha master write body error < 0");
                    }
                }
            }

            // 如果body写出去, 那么最终返回值, 取决于 header 和 body 都没有剩余的字节数据, 意味着数据全写出去了
            boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();

            // 全部写完以后, 释放 bufferResult, 并将成员变量 selectMappedBufferResult 置为null.
            if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                this.selectMappedBufferResult.release();
                this.selectMappedBufferResult = null;
            }

            return result;
        }

        @Override
        public String getServiceName() {
            return WriteSocketService.class.getSimpleName();
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }
    }
}
