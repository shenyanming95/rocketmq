package org.apache.rocketmq.broker.pagecache;

import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;
import org.apache.rocketmq.store.SelectMappedBufferResult;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class OneMessageTransfer extends AbstractReferenceCounted implements FileRegion {
    private final ByteBuffer byteBufferHeader;
    private final SelectMappedBufferResult selectMappedBufferResult;

    /**
     * Bytes which were transferred already.
     */
    private long transferred;

    public OneMessageTransfer(ByteBuffer byteBufferHeader, SelectMappedBufferResult selectMappedBufferResult) {
        this.byteBufferHeader = byteBufferHeader;
        this.selectMappedBufferResult = selectMappedBufferResult;
    }

    @Override
    public long position() {
        return this.byteBufferHeader.position() + this.selectMappedBufferResult.getByteBuffer().position();
    }

    @Override
    public long transfered() {
        return transferred;
    }

    @Override
    public long count() {
        return this.byteBufferHeader.limit() + this.selectMappedBufferResult.getSize();
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        if (this.byteBufferHeader.hasRemaining()) {
            transferred += target.write(this.byteBufferHeader);
            return transferred;
        } else if (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
            transferred += target.write(this.selectMappedBufferResult.getByteBuffer());
            return transferred;
        }

        return 0;
    }

    public void close() {
        this.deallocate();
    }

    @Override
    protected void deallocate() {
        this.selectMappedBufferResult.release();
    }
}
