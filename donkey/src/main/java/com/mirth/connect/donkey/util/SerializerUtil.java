package com.mirth.connect.donkey.util;

public class SerializerUtil {
    public static long bytesToLong(byte[] data) {
        long l = 0;
        l |= ((long)(0xff & data[7])) << 56;
        l |= ((long)(0xff & data[6])) << 48;
        l |= ((long)(0xff & data[5])) << 40;
        l |= ((long)(0xff & data[4])) << 32;
        l |= ((long)(0xff & data[3])) << 24;
        l |= ((long)(0xff & data[2])) << 16;
        l |= ((long)(0xff & data[1])) << 8;
        l |= ((long)(0xff & data[0]));
        
        return l;
    }

    public static byte[] longToBytes(long l) {
        byte[] data = new byte[8];

        data[0] = (byte)l;
        data[1] = (byte)(l >> 8);
        data[2] = (byte)(l >> 16);
        data[3] = (byte)(l >> 24);
        data[4] = (byte)(l >> 32);
        data[5] = (byte)(l >> 40);
        data[6] = (byte)(l >> 48);
        data[7] = (byte)(l >> 56);
        
        return data;
    }
}
