package com.mirth.connect.donkey.model.message;

import java.util.Calendar;

import com.mirth.connect.donkey.model.message.CapnpModel.CapConnectorMessage;

public class BdbJeConnectorMessage extends ConnectorMessage {
    private CapConnectorMessage.Reader cm;

    public BdbJeConnectorMessage(CapConnectorMessage.Reader cm) {
        this.cm = cm;
    }

    @Override
    public int getMetaDataId() {
        return cm.getId();
    }

    @Override
    public long getMessageId() {
        return cm.getMessageId();
    }

    @Override
    public String getServerId() {
        if(serverId == null) {
            serverId = cm.getServerId().toString();
        }
        return serverId;
    }

    @Override
    public String getConnectorName() {
        if(connectorName == null && cm.hasConnectorName()) {
            connectorName = cm.getConnectorName().toString();
        }

        return connectorName;
    }

    @Override
    public Calendar getReceivedDate() {
        if(receivedDate == null) {
            receivedDate = Calendar.getInstance();
            receivedDate.setTimeInMillis(cm.getReceivedDate());
        }

        return receivedDate;
    }

    @Override
    public Status getStatus() {
        if(status == null) {
            status = toConnectorMessageStatus(cm.getStatus());
        }
        return status;
    }

    @Override
    public int getErrorCode() {
        if(errorCode == 0) {
            errorCode = cm.getErrorCode();
        }
        return errorCode;
    }

    @Override
    public int getSendAttempts() {
        if(sendAttempts == 0) {
            sendAttempts = cm.getSendAttempts();
        }
        return sendAttempts;
    }

    @Override
    public Calendar getSendDate() {
        if(sendDate == null) {
            sendDate = Calendar.getInstance();
            sendDate.setTimeInMillis(cm.getSendDate());
        }

        return sendDate;
    }

    @Override
    public Calendar getResponseDate() {
        if(responseDate == null) {
            responseDate = Calendar.getInstance();
            responseDate.setTimeInMillis(cm.getResponseDate());
        }

        return responseDate;
    }

    @Override
    public int getChainId() {
        if(chainId == 0) {
            chainId = cm.getChainId();
        }
        return chainId;
    }

    @Override
    public int getOrderId() {
        if(orderId == 0) {
            orderId = cm.getOrderId();
        }
        return orderId;
    }
    
    public static Status toConnectorMessageStatus(CapConnectorMessage.CapStatus cstatus) {
        Status s = null;
        switch (cstatus) {
        case ERROR:
            s = Status.ERROR;
            break;
        case FILTERED:
            s = Status.FILTERED;
            break;
        case PENDING:
            s = Status.PENDING;
            break;
        case QUEUED:
            s = Status.QUEUED;
            break;
        case RECEIVED:
            s = Status.RECEIVED;
            break;
        case SENT:
            s = Status.SENT;
            break;
        case TRANSFORMED:
            s = Status.TRANSFORMED;
            break;
        }
        
        return s;
    }
}
