package org.apache.rocketmq.store;

import java.nio.ByteBuffer;

public class SelectMappedBufferResult {

    /**
     * 绝对位置, 即计算上{@link MappedFile#getFileFromOffset()}
     */
    private final long startOffset;

    /**
     * 实际数据
     */
    private final ByteBuffer byteBuffer;

    /**
     * 数据的可读大小
     */
    private int size;

    /**
     * 数据对应的载体
     */
    private MappedFile mappedFile;

    public SelectMappedBufferResult(long startOffset, ByteBuffer byteBuffer, int size, MappedFile mappedFile) {
        this.startOffset = startOffset;
        this.byteBuffer = byteBuffer;
        this.size = size;
        this.mappedFile = mappedFile;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public int getSize() {
        return size;
    }

    public void setSize(final int s) {
        this.size = s;
        this.byteBuffer.limit(this.size);
    }

//    @Override
//    protected void finalize() {
//        if (this.mappedFile != null) {
//            this.release();
//        }
//    }

    public synchronized void release() {
        if (this.mappedFile != null) {
            this.mappedFile.release();
            this.mappedFile = null;
        }
    }

    public long getStartOffset() {
        return startOffset;
    }
}
