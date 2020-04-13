package com.mirth.connect.donkey.server.data.jdbc;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.capnproto.BufferedOutputStream;

import com.sleepycat.je.DatabaseEntry;

import io.netty.buffer.ByteBuf;

public class CapnpOutputStream implements BufferedOutputStream {
    private ByteBuf buf;

    public CapnpOutputStream(ByteBuf buf) {
        this.buf = buf;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int n = src.remaining();
        buf.writeBytes(src);
        return n;
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public void close() throws IOException {
        buf.clear();
    }

    @Override
    public ByteBuffer getWriteBuffer() {
        return buf.nioBuffer();
    }

    @Override
    public void flush() throws IOException {
    }
    
    public DatabaseEntry getData() {
        //System.out.println("offset " + buf.arrayOffset() + " no. of bytes " + buf.readableBytes() + " iscontiguous " + buf.isContiguous());
        return new DatabaseEntry(buf.array(), buf.arrayOffset(), buf.readableBytes());
    }
}
