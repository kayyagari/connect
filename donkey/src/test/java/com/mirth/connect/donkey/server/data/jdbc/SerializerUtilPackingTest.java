package com.mirth.connect.donkey.server.data.jdbc;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.capnproto.ArrayOutputStream;
import org.capnproto.PackedOutputStream;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.mirth.connect.donkey.util.PackedByteBufStream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

@RunWith(Parameterized.class)
public class SerializerUtilPackingTest {
    private byte[] unpacked;
    private byte[] packed;

    public SerializerUtilPackingTest(byte[] unpacked, byte[] packed) {
        this.unpacked = unpacked;
        this.packed = packed;
    }
    
    @Parameters
    public static Object[][] data() {
        byte[] allOnesUnpacked = new byte[8 * 200];
        Arrays.fill(allOnesUnpacked, (byte)1);
        byte[] tmp = new byte[]{(byte)0xff, 1,1,1,1,1,1,1,1, (byte)199};
        byte[] allOnesPacked = new byte[tmp.length + (8 * 199)];
        System.arraycopy(tmp, 0, allOnesPacked, 0, tmp.length);
        Arrays.fill(allOnesPacked, tmp.length, allOnesPacked.length, (byte)1);
        
        Object[][] inputArrays = new Object[][] {
            {
                new byte[] {},
                new byte[] {}
            },
            
            {
                new byte[] {0,0,0,0,0,0,0,0},
                new byte[] {0,0}
            },
            {
                new byte[]{0,0,12,0,0,34,0,0},
                new byte[]{0x24,12,34}
             },
             {
                new byte[]{1,3,2,4,5,7,6,8},
                new byte[]{(byte)0xff,1,3,2,4,5,7,6,8,0}
             },
             {
                new byte[]{0,0,0,0,0,0,0,0, 1,3,2,4,5,7,6,8},
                new byte[]{0,0,(byte)0xff,1,3,2,4,5,7,6,8,0}
             },
             {
                new byte[]{0,0,12,0,0,34,0,0, 1,3,2,4,5,7,6,8},
                new byte[]{0x24, 12, 34, (byte)0xff,1,3,2,4,5,7,6,8,0}
             },
             {
                new byte[]{1,3,2,4,5,7,6,8, 8,6,7,4,5,2,3,1},
                new byte[]{(byte)0xff,1,3,2,4,5,7,6,8,1,8,6,7,4,5,2,3,1}
             },
             {
                new byte[]{1,2,3,4,5,6,7,8, 1,2,3,4,5,6,7,8, 1,2,3,4,5,6,7,8, 1,2,3,4,5,6,7,8, 0,2,4,0,9,0,5,1},
                new byte[]{(byte)0xff,1,2,3,4,5,6,7,8, 3, 1,2,3,4,5,6,7,8, 1,2,3,4,5,6,7,8, 1,2,3,4,5,6,7,8,(byte)0xd6,2,4,9,5,1}
             },
             {
                new byte[]{1,2,3,4,5,6,7,8, 1,2,3,4,5,6,7,8, 6,2,4,3,9,0,5,1, 1,2,3,4,5,6,7,8, 0,2,4,0,9,0,5,1},
                new byte[]{(byte)0xff,1,2,3,4,5,6,7,8, 3, 1,2,3,4,5,6,7,8, 6,2,4,3,9,0,5,1, 1,2,3,4,5,6,7,8,(byte)0xd6,2,4,9,5,1}
             },
             {
                new byte[]{8,0,100,6,0,1,1,2, 0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0, 0,0,1,0,2,0,3,1},
                new byte[]{(byte)0xed,8,100,6,1,1,2, 0,2, (byte)0xd4,1,2,3,1}
             },
             {
                new byte[]{0,0,0,0,2,0,0,0, 0,0,0,0,0,0,1,0, 0,0,0,0,0,0,0,0},
                new byte[]{0x10,2, 0x40,1, 0,0}
             },
             {
                 new byte[8*200],
                 new byte[] {0, (byte)199}
             },
             {
                 allOnesUnpacked,
                 allOnesPacked
             }
        };
        
        return inputArrays;
    }

    @Test
    public void test() throws Exception {
        ArrayOutputStream output = new ArrayOutputStream(ByteBuffer.allocate(2048));
        PackedOutputStream capPack = new PackedOutputStream(output);
        capPack.write(ByteBuffer.wrap(unpacked));
        ByteBuffer capBuf = output.getWriteBuffer();
        capBuf.flip();
        byte[] result = new byte[capBuf.remaining()];
        System.arraycopy(capBuf.array(), capBuf.position(), result, 0, capBuf.remaining());
        Assert.assertTrue(Arrays.equals(packed, result));
        capPack.close();

        ByteBuf buf = Unpooled.buffer(2048);
        PackedByteBufStream stream = new PackedByteBufStream(buf);
        stream.write(ByteBuffer.wrap(unpacked));
        result = new byte[buf.readableBytes()];
        System.arraycopy(buf.array(), buf.readerIndex(), result, 0, buf.readableBytes());
        Assert.assertTrue(Arrays.equals(packed, result));
    }
}
