package org.apache.rocketmq.filter.util;

import java.util.Arrays;

/**
 * Data generated by bloom filter, include:
 * <li>1. Bit positions allocated to requester;</li>
 * <li>2. Total bit num when allocating;</li>
 */
public class BloomFilterData {

    private int[] bitPos;
    private int bitNum;

    public BloomFilterData() {
    }

    public BloomFilterData(int[] bitPos, int bitNum) {
        this.bitPos = bitPos;
        this.bitNum = bitNum;
    }

    public int[] getBitPos() {
        return bitPos;
    }

    public void setBitPos(final int[] bitPos) {
        this.bitPos = bitPos;
    }

    public int getBitNum() {
        return bitNum;
    }

    public void setBitNum(final int bitNum) {
        this.bitNum = bitNum;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof BloomFilterData)) return false;

        final BloomFilterData that = (BloomFilterData) o;

        if (bitNum != that.bitNum) return false;
        if (!Arrays.equals(bitPos, that.bitPos)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = bitPos != null ? Arrays.hashCode(bitPos) : 0;
        result = 31 * result + bitNum;
        return result;
    }

    @Override
    public String toString() {
        return "BloomFilterData{" + "bitPos=" + Arrays.toString(bitPos) + ", bitNum=" + bitNum + '}';
    }
}
