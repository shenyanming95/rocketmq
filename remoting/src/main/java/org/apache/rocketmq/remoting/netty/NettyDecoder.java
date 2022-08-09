package org.apache.rocketmq.remoting.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.nio.ByteBuffer;

/**
 * 将TCP传输的字节数据解码成rocketMQ自定义的消息体{@link RemotingCommand}.
 * 有解码就有编码, 对应{@link NettyEncoder}
 */
public class NettyDecoder extends LengthFieldBasedFrameDecoder {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    private static final int FRAME_MAX_LENGTH = Integer.parseInt(System.getProperty("com.rocketmq.remoting.frameMaxLength", "16777216"));

    public NettyDecoder() {
        // LengthFieldBasedFrameDecoder 用来处理TCP粘包拆包问题, 它的五个参数含义分别为：

        // 1.maxFrameLength: 数据帧的最大长度, rocketMQ默认的设置为16777216;
        // 2.lengthFieldOffset: 自定义协议中, 一般会有一个length字段用于存储消息体的大小, 这个参数就是用来表示length字段在协议中的偏移量, 若为0说明length字段是在协议中的第一个字节开始表示的
        // 3.lengthFieldLength: 上面所说的length字段的长度, 它的值用于指明body的大小
        // 4.lengthAdjustment: 这个参数最复杂, 原理是这样子, netty利用 lengthFieldOffset 和 和lengthFieldLength 读取到消息长度之后, 它是指从 length 字段出发
        //                     到数据包结束需要多少个字节(假设lengthFieldLength = 10), 那么在读取完 length 字段后 netty 会继续等待13个字节到达后, 再返回成一个包.
        //                     但是某些协议, length 字段的值表示的是整个数据包的大小, 因此当Netty读出Length字段的时候(假设存储length字段的长度为4), 它的值为10，
        //                     那么netty还是会继续等待10个字节, 但实际上这个length的值是10代表的是整个数据包的长度, 扣去length的4个字节, 实际的body长度只有6个字节.
        //                     此时就可以将这个值设置为-4, 让netty用10-4=6, 继续等待6个字节即可.
        // 5.initialBytesToStrip: 需要跳过的字节数, 一般netty解析出来会按照header+body的形式, 但是往往只需要body数据, 所以就跳过header长度让netty直接返回body即可.
        super(FRAME_MAX_LENGTH, 0, 4, 0, 4);
    }

    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = null;
        try {
            frame = (ByteBuf) super.decode(ctx, in);
            if (null == frame) {
                return null;
            }
            // 解析字节数组, 其实就是按照消息格式依次解析一定数量的字节数据
            ByteBuffer byteBuffer = frame.nioBuffer();
            return RemotingCommand.decode(byteBuffer);
        } catch (Exception e) {
            log.error("decode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
            RemotingUtil.closeChannel(ctx.channel());
        } finally {
            if (null != frame) {
                frame.release();
            }
        }
        return null;
    }
}
