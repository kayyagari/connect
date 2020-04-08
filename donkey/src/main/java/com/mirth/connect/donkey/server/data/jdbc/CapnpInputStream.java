package com.mirth.connect.donkey.server.data.jdbc;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.capnproto.BufferedInputStream;

public class CapnpInputStream implements BufferedInputStream {

    private byte[] data;
    private boolean read = false;
    public CapnpInputStream(byte[] data) {
        this.data = data;
    }
    
    @Override
    public int read(ByteBuffer dst) throws IOException {
        if(read) {
            return -1;
        }

        dst.put(data);
        read = true;
        return data.length;
    }

    @Override
    public boolean isOpen() {
        return !read;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public ByteBuffer getReadBuffer() throws IOException {
        throw new UnsupportedOperationException("not supported");
    }
}
