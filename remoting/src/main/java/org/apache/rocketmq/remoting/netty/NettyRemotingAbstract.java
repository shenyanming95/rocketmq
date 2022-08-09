package org.apache.rocketmq.remoting.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.common.ServiceThread;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;

import java.net.SocketAddress;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

/**
 * 一个对端节点的抽象, 即在CS架构中, client和server都共用这个类.
 * rocketMQ将网络节点的共同操作抽象出来
 */
public abstract class NettyRemotingAbstract {

    /**
     * Remoting logger instance.
     */
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    static {
        NettyLogger.initNettyLogger();
    }

    /**
     * 用信号量限制正在进行的单向请求的最大数量, 保护系统内存占用
     */
    protected final Semaphore semaphoreOneway;

    /**
     * 用信号量限制正在进行的异步请求的最大数量, 保护系统内存占用
     */
    protected final Semaphore semaphoreAsync;

    /**
     * 缓存正在进行中的请求
     */
    protected final ConcurrentMap<Integer /* opaque */, ResponseFuture> responseTable = new ConcurrentHashMap<Integer, ResponseFuture>(256);

    /**
     * 存储每个请求码的所有处理器, 对于每个请求码 {@link org.apache.rocketmq.common.protocol.RequestCode},
     * 都可以在这个容器中查找对于的请求处理器{@link NettyRequestProcessor}来处理请求.
     * 填充操作发生在{@link org.apache.rocketmq.broker.BrokerController#registerProcessor()}中
     */
    protected final HashMap<Integer/* request code */, Pair<NettyRequestProcessor, ExecutorService>> processorTable = new HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>>(64);

    /**
     * 将netty事件传递给用户自定义的监听器{@link ChannelEventListener}
     */
    protected final NettyEventExecutor nettyEventExecutor = new NettyEventExecutor();

    /**
     * 如果一个请求码在 {@link #processorTable} 中没有完全匹配的请求处理器, 那么就使用下面这个默认的处理器
     */
    protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessor;

    /**
     * 用于创建{@link SslHandler}的SSL上下文
     */
    protected volatile SslContext sslContext;

    /**
     * 自定义的RPC钩子函数, 在执行RPC请求前后被回调
     */
    protected List<RPCHook> rpcHooks = new ArrayList<RPCHook>();

    /**
     * 构造方法, 指定单向和异步信号量的容量
     *
     * @param permitsOneway 单向请求的许可数
     * @param permitsAsync  异步请求的许可数
     */
    public NettyRemotingAbstract(final int permitsOneway, final int permitsAsync) {
        this.semaphoreOneway = new Semaphore(permitsOneway, true);
        this.semaphoreAsync = new Semaphore(permitsAsync, true);
    }

    /**
     * 自定义的通道监听器
     *
     * @return 如果定义了, 返回监听器, 否则返回null
     */
    public abstract ChannelEventListener getChannelEventListener();

    /**
     * 指定执行{@link InvokeCallback}回调方法的线程池
     *
     * @return 如果为null, 会用当前线程执行回调逻辑, 那么应该就是netty nio-loop线程.
     */
    public abstract ExecutorService getCallbackExecutor();

    /**
     * 新增一个netty事件
     *
     * @param event Netty event instance.
     */
    public void putNettyEvent(final NettyEvent event) {
        this.nettyEventExecutor.putNettyEvent(event);
    }

    /**
     * <pre>
     *     处理当前节点收到的命令, 分为两种情况：
     *     1.如果当前节点是client, 那么这个命令就是当前节点发出请求后server的响应;
     *     2.如果当前节点是server, 那么这个命令就是client节点发出的请求.
     * </pre>
     * <pre>
     *     Entry of incoming command processing. The incoming remoting command may be:
     *     1.An inquiry request from a remote peer component;
     *     2.A response to a previous request issued by this very participant.
     * </pre>
     *
     * @param ctx netty组件, 通道上下文
     * @param msg 待处理的命令
     * @throws Exception 处理过程中出现的异常
     */
    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
        final RemotingCommand cmd = msg;
        if (cmd != null) {
            switch (cmd.getType()) {
                case REQUEST_COMMAND:
                    // 处理client发出的请求
                    processRequestCommand(ctx, cmd);
                    break;
                case RESPONSE_COMMAND:
                    // 处理此前发出, 收到的server的响应
                    processResponseCommand(ctx, cmd);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * 处理client发出的请求命令.
     * 如果server无法执行的命令, rocketMQ会选择立即写回响应信息给对端.
     *
     * @param ctx channel handler context.
     * @param cmd request command.
     */
    public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        // 拿到这个请求对应的处理器
        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
        // 如果拿不到就使用默认的处理器
        final Pair<NettyRequestProcessor, ExecutorService> pair = (null == matched ? this.defaultRequestProcessor : matched);
        // 请求id, 原子递增
        final int opaque = cmd.getOpaque();

        if (pair != null) {
            // 将处理逻辑封装到Runnable里
            Runnable run = new Runnable() {
                @Override
                public void run() {
                    try {
                        // 执行rpc服务之前调用钩子函数
                        doBeforeRpcHooks(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd);
                        // 创建对响应命令的回调处理逻辑
                        final RemotingResponseCallback callback = new RemotingResponseCallback() {
                            @Override
                            public void callback(RemotingCommand response) {
                                // 执行rpc服务之后调用钩子函数
                                doAfterRpcHooks(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd, response);
                                // 需要响应数据给client
                                if (!cmd.isOnewayRPC()) {
                                    if (response != null) {
                                        // 设置都request中取到的请求唯一标识
                                        response.setOpaque(opaque);
                                        // 标志此命令为响应命令
                                        response.markResponseType();
                                        try {
                                            // 将响应结果写回通道给client
                                            ctx.writeAndFlush(response);
                                        } catch (Throwable e) {
                                            log.error("process request over, but response failed", e);
                                            log.error(cmd.toString());
                                            log.error(response.toString());
                                        }
                                    } else {
                                    }
                                }
                            }
                        };
                        // 区分同步处理还是异步处理, 此处的线程池是业务线程池.
                        // 当前版本的rocketMQ只有 org.apache.rocketmq.broker.processor.SendMessageProcessor 才有真正意义上的异步处理
                        if (pair.getObject1() instanceof AsyncNettyRequestProcessor) {
                            AsyncNettyRequestProcessor processor = (AsyncNettyRequestProcessor) pair.getObject1();
                            processor.asyncProcessRequest(ctx, cmd, callback);
                        } else {
                            NettyRequestProcessor processor = pair.getObject1();
                            RemotingCommand response = processor.processRequest(ctx, cmd);
                            callback.callback(response);
                        }
                    } catch (Throwable e) {
                        log.error("process request exception", e);
                        log.error(cmd.toString());

                        if (!cmd.isOnewayRPC()) {
                            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, RemotingHelper.exceptionSimpleDesc(e));
                            response.setOpaque(opaque);
                            ctx.writeAndFlush(response);
                        }
                    }
                }
            };

            // 拒绝处理请求, 响应对端数据
            if (pair.getObject1().rejectRequest()) {
                final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY, "[REJECTREQUEST]system busy, start flow control for a while");
                response.setOpaque(opaque);
                ctx.writeAndFlush(response);
                return;
            }

            try {
                // 将处理逻辑包裹成一个任务, 异步执行, 此处的线程池是I/O线程池, 影响到是否可以及时响应client的请求
                final RequestTask requestTask = new RequestTask(run, ctx.channel(), cmd);
                pair.getObject2().submit(requestTask);
            } catch (RejectedExecutionException e) {

                // 线程池繁忙, 拒绝处理本次请求.
                // 保证10s打印一次系统繁忙日志
                if ((System.currentTimeMillis() % 10000) == 0) {
                    log.warn(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + ", too many requests and system thread pool busy, RejectedExecutionException " + pair.getObject2().toString() + " request code: " + cmd.getCode());
                }

                // 如果请求命令需要响应, 那么需要写回信息
                if (!cmd.isOnewayRPC()) {
                    final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY, "[OVERLOAD]system busy, start flow control for a while");
                    response.setOpaque(opaque);
                    ctx.writeAndFlush(response);
                }
            }
        } else {
            // 当前请求的请求码不被支持
            String error = " request type " + cmd.getCode() + " not supported";
            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
            response.setOpaque(opaque);
            ctx.writeAndFlush(response);
            log.error(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + error);
        }
    }

    /**
     * 处理server发回的请求命令
     *
     * @param ctx netty通道上下文
     * @param cmd sever的响应
     */
    public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        // server返回的RemotingCommand里的opaque, 用的就是之前发出的RemotingCommand里的opaque
        final int opaque = cmd.getOpaque();
        // 通过opaque拿到响应future
        final ResponseFuture responseFuture = responseTable.get(opaque);
        if (responseFuture != null) {
            // 设置响应命令
            responseFuture.setResponseCommand(cmd);
            // 移除对应的命令, 表示请求已经处理
            responseTable.remove(opaque);

            // 如果用户选择异步处理, 那么就丢到线程池执行future.
            // 否则, 直接唤醒阻塞在这个future的线程.
            if (responseFuture.getInvokeCallback() != null) {
                executeInvokeCallback(responseFuture);
            } else {
                responseFuture.putResponse(cmd);
                responseFuture.release();
            }
        } else {
            log.warn("receive response, but not matched any request, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            log.warn(cmd.toString());
        }
    }

    /**
     * Custom RPC hooks.
     *
     * @return RPC hooks if specified; null otherwise.
     */
    public List<RPCHook> getRPCHooks() {
        return rpcHooks;
    }

    /**
     * 定期调用此方法来扫描和过期已弃用的请求
     */
    public void scanResponseTable() {
        final List<ResponseFuture> rfList = new LinkedList<ResponseFuture>();
        Iterator<Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, ResponseFuture> next = it.next();
            ResponseFuture rep = next.getValue();
            // responseFuture已经过期, 释放它, 并将它从responseTable中剔除掉.
            if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000) <= System.currentTimeMillis()) {
                rep.release();
                it.remove();
                rfList.add(rep);
                log.warn("remove timeout request, " + rep);
            }
        }

        // 被剔除的future还是会选择执行它的回调逻辑
        for (ResponseFuture rf : rfList) {
            try {
                executeInvokeCallback(rf);
            } catch (Throwable e) {
                log.warn("scanResponseTable, operationComplete Exception", e);
            }
        }
    }

    /**
     * 同步处理请求.（双向请求, 需要对端响应）
     *
     * @param channel       TCP通道
     * @param request       需要发送给对端的请求
     * @param timeoutMillis 超时时间
     */
    public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        // 当前请求的标识
        final int opaque = request.getOpaque();
        try {
            // 创建一个响应的future, 然后将其与请求的唯一标识关联起来
            final ResponseFuture responseFuture = new ResponseFuture(channel, opaque, timeoutMillis, null, null);
            // 先将future放到缓存里面, 发送完请求后, 会从这里通过opaque拿到对应的future进行处理.
            this.responseTable.put(opaque, responseFuture);
            // 获取对端的IP地址
            final SocketAddress addr = channel.remoteAddress();
            // 将请求发送给对端, 然后添加一个监听器
            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                    // 请求发送成功后回调, 设置状态
                    if (f.isSuccess()) {
                        responseFuture.setSendRequestOK(true);
                        return;
                    } else {
                        responseFuture.setSendRequestOK(false);
                    }
                    // 发送失败后设置状态
                    responseTable.remove(opaque);
                    responseFuture.setCause(f.cause());
                    responseFuture.putResponse(null);
                    log.warn("send a request command to channel <" + addr + "> failed.");
                }
            });
            // 等待请求的执行完成. 注意请求发出后, 需要TCP对端有响应, 然后是在 NettyRemotingAbstract#processMessageReceived()方法处理对端发来的数据.
            RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);
            // 为空说明请求没有正常交互成功. 若已发送request, 那么说明是TCP对端没有及时返回数据; 如果没有发送, 则说明是本端发送request出现问题.
            // 这两种情况rocketMQ都选择抛异常
            if (null == responseCommand) {
                if (responseFuture.isSendRequestOK()) {
                    throw new RemotingTimeoutException(RemotingHelper.parseSocketAddressAddr(addr), timeoutMillis, responseFuture.getCause());
                } else {
                    throw new RemotingSendRequestException(RemotingHelper.parseSocketAddressAddr(addr), responseFuture.getCause());
                }
            }
            // 有数据则直接返回
            return responseCommand;
        } finally {
            // 必须要移除这个future, 防止内存泄露
            this.responseTable.remove(opaque);
        }
    }

    /**
     * 异步处理请求.（双向请求, 需要对端响应）
     *
     * @param channel        TCP通道
     * @param request        需要发送给对端的请求
     * @param timeoutMillis  超时时间
     * @param invokeCallback 回调逻辑
     */
    public void invokeAsyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis, final InvokeCallback invokeCallback) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        long beginStartTime = System.currentTimeMillis();
        final int opaque = request.getOpaque();
        // rocketMQ对异步请求有限制, 它通过JDK提供信号量来实现
        boolean acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);
            // 拿信号量耗费的时间已经比外部传入的超时时间还大, 直接抛出超时异常
            long costTime = System.currentTimeMillis() - beginStartTime;
            if (timeoutMillis < costTime) {
                once.release();
                throw new RemotingTimeoutException("invokeAsyncImpl call timeout");
            }
            // 将request和response关联起来
            final ResponseFuture responseFuture = new ResponseFuture(channel, opaque, timeoutMillis - costTime, invokeCallback, once);
            this.responseTable.put(opaque, responseFuture);
            try {
                // 将请求发送给对端, 然后添加一个监听器
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        // 发送成功, 设置发送状态为ok
                        if (f.isSuccess()) {
                            responseFuture.setSendRequestOK(true);
                            return;
                        }
                        // 发送失败, 调整发送状态, 并且执行回调函数
                        requestFail(opaque);
                        log.warn("send a request command to channel <{}> failed.", RemotingHelper.parseChannelRemoteAddr(channel));
                    }
                });
                // 这里就不像上一个方法invokeSyncImpl()会在这边同步等待对端的响应,
                // 而是直接将方法返回.
            } catch (Exception e) {
                responseFuture.release();
                log.warn("send a request command to channel <" + RemotingHelper.parseChannelRemoteAddr(channel) + "> Exception", e);
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            // 信号量获取失败
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fast");
            } else {
                String info = String.format("invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d", timeoutMillis, this.semaphoreAsync.getQueueLength(), this.semaphoreAsync.availablePermits());
                log.warn(info);
                throw new RemotingTimeoutException(info);
            }
        }
    }

    /**
     * 异步处理请求.（单向请求, 无需对端响应）
     *
     * @param channel       TCP通道
     * @param request       需要发送给对端的请求
     * @param timeoutMillis 超时时间
     */
    public void invokeOnewayImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        request.markOnewayRPC();
        // rocketMQ对单向请求有限制, 它通过JDK提供信号量来实现
        boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);
            try {
                // 只要负责发出请求即可.
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        once.release();
                        if (!f.isSuccess()) {
                            log.warn("send a request command to channel <" + channel.remoteAddress() + "> failed.");
                        }
                    }
                });
            } catch (Exception e) {
                once.release();
                log.warn("write send a request command to channel <" + channel.remoteAddress() + "> failed.");
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeOnewayImpl invoke too fast");
            } else {
                String info = String.format("invokeOnewayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d", timeoutMillis, this.semaphoreOneway.getQueueLength(), this.semaphoreOneway.availablePermits());
                log.warn(info);
                throw new RemotingTimeoutException(info);
            }
        }
    }

    /**
     * 在执行命令之前, 回调钩子函数
     *
     * @param addr    远程对端地址
     * @param request 远程对端发出的请求
     */
    protected void doBeforeRpcHooks(String addr, RemotingCommand request) {
        if (rpcHooks.size() > 0) {
            for (RPCHook rpcHook : rpcHooks) {
                rpcHook.doBeforeRequest(addr, request);
            }
        }
    }

    /**
     * 在执行命令之后, 回调钩子函数
     *
     * @param addr     远程对端地址
     * @param request  远程对端发出的请求
     * @param response 处理请求后的响应
     */
    protected void doAfterRpcHooks(String addr, RemotingCommand request, RemotingCommand response) {
        if (rpcHooks.size() > 0) {
            for (RPCHook rpcHook : rpcHooks) {
                rpcHook.doAfterResponse(addr, request, response);
            }
        }
    }

    /**
     * 将参数channel对应的请求{@link ResponseFuture}标记为失败, 并且立即执行它的回调方法
     *
     * @param channel 已经关闭了的netty通道
     */
    protected void failFast(final Channel channel) {
        Iterator<Entry<Integer, ResponseFuture>> it = responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, ResponseFuture> entry = it.next();
            // 通过channel能找到responseFuture的关键就是:
            // org.apache.rocketmq.remoting.netty.ResponseFuture.processChannel,
            // responseFuture持有执行它的channel引用.
            if (entry.getValue().getProcessChannel() == channel) {
                Integer opaque = entry.getKey();
                if (opaque != null) {
                    requestFail(opaque);
                }
            }
        }
    }

    /**
     * Custom RPC hook.
     * Just be compatible with the previous version, use getRPCHooks instead.
     */
    @Deprecated
    protected RPCHook getRPCHook() {
        if (rpcHooks.size() > 0) {
            return rpcHooks.get(0);
        }
        return null;
    }

    /**
     * 调用{@link ResponseFuture#invokeCallback}逻辑, 如果配置了
     * 回调线程池, 则在线程池中回调; 否则直接在当前线程执行
     */
    private void executeInvokeCallback(final ResponseFuture responseFuture) {
        boolean runInThisThread = false;
        ExecutorService executor = this.getCallbackExecutor();
        if (executor != null) {
            try {
                // 回调线程池不为空, 丢到线程池中执行
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            // 执行回调函数
                            responseFuture.executeInvokeCallback();
                        } catch (Throwable e) {
                            log.warn("execute callback in executor exception, and callback throw", e);
                        } finally {
                            responseFuture.release();
                        }
                    }
                });
            } catch (Exception e) {
                // 回调线程池执行出错(大概率是因为线程池已满, 无法提交任务), 那么会让当前线程执行
                runInThisThread = true;
                log.warn("execute callback in executor exception, maybe executor busy", e);
            }
        } else {
            // 压根没有回调线程池, 就直接用当前线程执行
            runInThisThread = true;
        }

        // 为true, 表示需要在当前线程中执行
        if (runInThisThread) {
            try {
                responseFuture.executeInvokeCallback();
            } catch (Throwable e) {
                log.warn("executeInvokeCallback Exception", e);
            } finally {
                responseFuture.release();
            }
        }
    }

    /**
     * 异步方式发送request给TCP对端失败时, 处理此次request.
     * 通过 opaque 找到{@link ResponseFuture}, 将其置为失败状态.
     */
    private void requestFail(final int opaque) {
        ResponseFuture responseFuture = responseTable.remove(opaque);
        if (responseFuture != null) {
            responseFuture.setSendRequestOK(false);
            responseFuture.putResponse(null);
            try {
                // 执行回调
                executeInvokeCallback(responseFuture);
            } catch (Throwable e) {
                log.warn("execute callback in requestFail, and callback throw", e);
            } finally {
                responseFuture.release();
            }
        }
    }

    /**
     * 专门处理netty事件的线程池
     */
    class NettyEventExecutor extends ServiceThread {
        private final LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<NettyEvent>();
        private final int maxSize = 10000;

        public void putNettyEvent(final NettyEvent event) {
            if (this.eventQueue.size() <= maxSize) {
                this.eventQueue.add(event);
            } else {
                log.warn("event queue size[{}] enough, so drop this event {}", this.eventQueue.size(), event.toString());
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            final ChannelEventListener listener = NettyRemotingAbstract.this.getChannelEventListener();

            while (!this.isStopped()) {
                try {
                    NettyEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                    if (event != null && listener != null) {
                        switch (event.getType()) {
                            case IDLE:
                                listener.onChannelIdle(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CLOSE:
                                listener.onChannelClose(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CONNECT:
                                listener.onChannelConnect(event.getRemoteAddr(), event.getChannel());
                                break;
                            case EXCEPTION:
                                listener.onChannelException(event.getRemoteAddr(), event.getChannel());
                                break;
                            default:
                                break;

                        }
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return NettyEventExecutor.class.getSimpleName();
        }
    }
}
