package org.apache.rocketmq.filter;

import org.apache.rocketmq.filter.util.BitsArray;
import org.junit.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class BitsArrayTest {

    int bitLength = Byte.SIZE;

    BitsArray gen(int bitCount) {
        BitsArray bitsArray = BitsArray.create(bitCount);

        for (int i = 0; i < bitCount / Byte.SIZE; i++) {
            bitsArray.setByte(i, (byte) (new Random(System.currentTimeMillis())).nextInt(0xff));
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
            }
        }

        return bitsArray;
    }

    @Test
    public void testConstructor() {
        BitsArray bitsArray = BitsArray.create(8);

        assertThat(bitsArray.byteLength() == 1 && bitsArray.bitLength() == 8).isTrue();

        bitsArray = BitsArray.create(9);

        assertThat(bitsArray.byteLength() == 2 && bitsArray.bitLength() == 9).isTrue();

        bitsArray = BitsArray.create(7);

        assertThat(bitsArray.byteLength() == 1 && bitsArray.bitLength() == 7).isTrue();
    }

    @Test
    public void testSet() {
        BitsArray bitsArray = gen(bitLength);
        BitsArray backUp = bitsArray.clone();

        boolean val = bitsArray.getBit(2);

        bitsArray.setBit(2, !val);

        bitsArray.xor(backUp);

        assertThat(bitsArray.getBit(2)).isTrue();
    }

    @Test
    public void testAndOr() {
        BitsArray bitsArray = gen(bitLength);

        boolean val = bitsArray.getBit(2);

        if (val) {
            bitsArray.and(2, false);
            assertThat(!bitsArray.getBit(2)).isTrue();
        } else {
            bitsArray.or(2, true);
            assertThat(bitsArray.getBit(2)).isTrue();
        }
    }

    @Test
    public void testXor() {
        BitsArray bitsArray = gen(bitLength);

        boolean val = bitsArray.getBit(2);

        bitsArray.xor(2, !val);

        assertThat(bitsArray.getBit(2)).isTrue();
    }

    @Test
    public void testNot() {
        BitsArray bitsArray = gen(bitLength);
        BitsArray backUp = bitsArray.clone();

        bitsArray.not(2);

        bitsArray.xor(backUp);

        assertThat(bitsArray.getBit(2)).isTrue();
    }

    @Test
    public void testOr() {
        BitsArray b1 = BitsArray.create(new byte[]{(byte) 0xff, 0x00});
        BitsArray b2 = BitsArray.create(new byte[]{0x00, (byte) 0xff});

        b1.or(b2);

        for (int i = 0; i < b1.bitLength(); i++) {
            assertThat(b1.getBit(i)).isTrue();
        }
    }
}
