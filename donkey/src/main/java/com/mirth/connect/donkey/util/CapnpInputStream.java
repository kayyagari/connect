package com.mirth.connect.donkey.util;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.capnproto.DecodeException;

// adapted from the org.capnproto.PackedInputStream
// this version avoids creating multiple slices and also there is no external InputStream
// all the data to be read is available at once in a byte array
public class CapnpInputStream {
    private ByteBuffer inner;
    private byte[] data;

    public CapnpInputStream(byte[] data) {
        this.data = data;
        this.inner = ByteBuffer.wrap(data);
    }

    public int read(ByteBuffer outBuf) throws IOException {

        int len = outBuf.remaining();
        if (len == 0) {
            return 0;
        }

        if (len % 8 != 0) {
            throw new DecodeException(
                    "PackedInputStream reads must be word-aligned");
        }

        int outPtr = outBuf.position();
        int outEnd = outPtr + len;

        ByteBuffer inBuf = this.inner;

        while (true) {

            byte tag = 0;

            if (inBuf.remaining() < 10) {
                if (outBuf.remaining() == 0) {
                    return len;
                }

                //# We have at least 1, but not 10, bytes available. We need to read
                //# slowly, doing a bounds check on each byte.

                tag = inBuf.get();

                for (int i = 0; i < 8; ++i) {
                    if ((tag & (1 << i)) != 0) {
                        outBuf.put(inBuf.get());
                    } else {
                        outBuf.put((byte)0);
                    }
                }
            }
            else {
                tag = inBuf.get();
                for (int n = 0; n < 8; ++n) {
                    boolean isNonzero = (tag & (1 << n)) != 0;
                    outBuf.put((byte) (inBuf.get() & (isNonzero ? -1 : 0)));
                    inBuf.position(inBuf.position() + (isNonzero ? 0 : -1));
                }
            }

            if (tag == 0) {
                if (inBuf.remaining() == 0) {
                    throw new DecodeException(
                            "Should always have non-empty buffer here.");
                }

                int runLength = (0xff & (int) inBuf.get()) * 8;

                if (runLength > outEnd - outPtr) {
                    throw new DecodeException(
                            "Packed input did not end cleanly on a segment boundary");
                }

                for (int i = 0; i < runLength; ++i) {
                    outBuf.put((byte) 0);
                }
            } else if (tag == (byte) 0xff) {

                int runLength = (0xff & (int) inBuf.get()) * 8;

                int pos = inBuf.position();
                outBuf.put(data, pos, runLength);
                inBuf.position(pos + runLength);
            }

            if (outBuf.remaining() == 0) {
                return len;
            }
        }
    }

    public void close() throws IOException {
    }

    public boolean isOpen() {
        return inner.hasRemaining();
    }
}
