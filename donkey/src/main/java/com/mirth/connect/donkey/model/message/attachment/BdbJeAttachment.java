package com.mirth.connect.donkey.model.message.attachment;

import com.mirth.connect.donkey.model.message.CapnpModel.CapAttachment;

public class BdbJeAttachment extends Attachment {
    private CapAttachment.Reader cr;

    public BdbJeAttachment(CapAttachment.Reader cr) {
        this.cr = cr;
    }

    @Override
    public String getAttachmentId() {
        if(id == null) {
            id = cr.getId().toString();
        }

        return super.getAttachmentId();
    }

    @Override
    public String getId() {
        if(id == null) {
            id = cr.getId().toString();
        }

        return id;
    }

    @Override
    public byte[] getContent() {
        if(content == null) {
            content = cr.getContent().toArray();
        }

        return content;
    }

    @Override
    public String getType() {
        if(type == null) {
            type = cr.getType().toString();
        }

        return type;
    }
}
