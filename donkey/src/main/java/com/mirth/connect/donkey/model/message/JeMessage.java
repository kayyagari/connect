package com.mirth.connect.donkey.model.message;

import java.util.Calendar;

import com.mirth.connect.donkey.model.message.CapnpModel.CapMessage;

public class JeMessage extends Message {
    private CapMessage.Reader cm;
    public JeMessage() {
        super();
    }
    
    public JeMessage(CapMessage.Reader cm) {
        this.cm = cm;
    }

    @Override
    public Long getMessageId() {
        if(messageId == 0) {
            messageId = cm.getMessageId();
        }
        return messageId;
    }

    @Override
    public String getServerId() {
        if(serverId == null) {
            serverId = cm.getServerId().toString();
        }
        return serverId;
    }

    @Override
    public String getChannelId() {
        if(channelId == null) {
            channelId = cm.getChannelId().toString();
        }
        return channelId;
    }

    @Override
    public Calendar getReceivedDate() {
        if(receivedDate == null) {
            long t = cm.getReceivedDate();
            receivedDate = Calendar.getInstance();
            receivedDate.setTimeInMillis(t);
        }

        return receivedDate;
    }

    @Override
    public boolean isProcessed() {
        return cm.getProcessed();
    }

    @Override
    public Long getOriginalId() {
        if(originalId == 0) {
            originalId = cm.getOriginalId();;
        }
        return originalId;
    }

    @Override
    public Long getImportId() {
        if(importId == 0) {
            importId = cm.getImportId();
        }
        return importId;
    }

    @Override
    public String getImportChannelId() {
        if(importChannelId == null && cm.hasImportChannelId()) {
            importChannelId = cm.getImportChannelId().toString();
        }
        
        return importChannelId;
    }
}
