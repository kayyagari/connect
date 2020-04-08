package com.mirth.connect.donkey.server.data.jdbc;

import org.capnproto.MessageBuilder;
import org.capnproto.StructBuilder;

public class ReusableMessageBuilder {
    private MessageBuilder mb;
    private StructBuilder sb;

    public ReusableMessageBuilder(MessageBuilder mb, StructBuilder sb) {
        this.mb = mb;
        this.sb = sb;
    }

    public void reset() {
        mb.clearFirstSegment();
    }
    
    public StructBuilder getSb() {
        return sb;
    }
    
    public MessageBuilder getMb() {
        return mb;
    }
}
