package com.mirth.connect.donkey.server.data.jdbc;

import java.nio.ByteBuffer;

import org.capnproto.MessageBuilder;
import org.capnproto.StructBuilder;
import org.capnproto.StructFactory;

public class ReusableMessageBuilder {
    private MessageBuilder mb;
    private StructBuilder sb;
    private ByteBuffer backingBuf;
    private StructFactory factory;

    public ReusableMessageBuilder(ByteBuffer backingBuf, StructFactory factory) {
        this.backingBuf = backingBuf;
        this.factory = factory;
        mb = new MessageBuilder(this.backingBuf);
        sb = (StructBuilder) mb.initRoot(this.factory);
    }

    public void reset() {
        mb.clearFirstSegment();
        mb = new MessageBuilder(backingBuf);
        sb = (StructBuilder) mb.initRoot(factory);
    }
    
    public StructBuilder getSb() {
        return sb;
    }
    
    public MessageBuilder getMb() {
        return mb;
    }
    
    public int getInitBufSize() {
        return backingBuf.capacity();
    }
}
