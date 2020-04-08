package com.mirth.connect.donkey.util;

public class SerializerUtil {
    public static long bytesToLong(byte[] buf) {
        return bytesToLong(buf, 0);
    }
    
    public static long bytesToLong(byte[] data, int offset) {
        long l = 0;
        l |= ((long)(0xff & data[offset + 7])) << 56;
        l |= ((long)(0xff & data[offset + 6])) << 48;
        l |= ((long)(0xff & data[offset + 5])) << 40;
        l |= ((long)(0xff & data[offset + 4])) << 32;
        l |= ((long)(0xff & data[offset + 3])) << 24;
        l |= ((long)(0xff & data[offset + 2])) << 16;
        l |= ((long)(0xff & data[offset + 1])) << 8;
        l |= ((long)(0xff & data[offset]));
        
        return l;
    }

    public static byte[] longToBytes(long l) {
        byte[] buf = new byte[8];
        longToBytes(l, buf, 0);
        return buf;
    }
    
    public static void longToBytes(long l, byte[] buf, int offset) {
        buf[offset] = (byte)l;
        buf[offset + 1] = (byte)(l >> 8);
        buf[offset + 2] = (byte)(l >> 16);
        buf[offset + 3] = (byte)(l >> 24);
        buf[offset + 4] = (byte)(l >> 32);
        buf[offset + 5] = (byte)(l >> 40);
        buf[offset + 6] = (byte)(l >> 48);
        buf[offset + 7] = (byte)(l >> 56);
    }
    
    public static byte[] intToBytes(int n) {
        byte[] buf = new byte[4];
        intToBytes(n, buf, 0);
        return buf;
    }

    public static int bytesToInt(byte[] buf) {
        return bytesToInt(buf, 0);
    }

    public static int bytesToInt(byte[] buf, int offset) {
        int n = 0;
        n |= ((int)(0xff & buf[offset+3])) << 24;
        n |= ((int)(0xff & buf[offset+2])) << 16;
        n |= ((int)(0xff & buf[offset+1])) << 8;
        n |= ((int)(0xff & buf[offset]));
        
        return n;
    }

    public static void intToBytes(int n, byte[] buf, int offset) {
        buf[offset] = (byte)n;
        buf[offset+1] = (byte)(n >> 8);
        buf[offset+2] = (byte)(n >> 16);
        buf[offset+3] = (byte)(n >> 24);
    }
}
