/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.donkey.model.message;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.mirth.connect.donkey.model.message.attachment.Attachment;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("message")
public class Message implements Serializable {
    protected Long messageId;
    protected String serverId;
    protected String channelId;
    protected Calendar receivedDate;
    protected boolean processed;
    protected Long originalId;
    protected Long importId;
    protected String importChannelId;
    protected List<Attachment> attachments;
    protected Map<Integer, ConnectorMessage> connectorMessages = new LinkedHashMap<Integer, ConnectorMessage>();
    protected transient ConnectorMessage mergedConnectorMessage;

    public Long getMessageId() {
        return messageId;
    }

    public void setMessageId(Long messageId) {
        this.messageId = messageId;
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public Calendar getReceivedDate() {
        return receivedDate;
    }

    public void setReceivedDate(Calendar receivedDate) {
        this.receivedDate = receivedDate;
    }

    public boolean isProcessed() {
        return processed;
    }

    public void setProcessed(boolean processed) {
        this.processed = processed;
    }

    public Long getOriginalId() {
        return originalId;
    }

    public void setOriginalId(Long originalId) {
        this.originalId = originalId;
    }

    public Long getImportId() {
        return importId;
    }

    public void setImportId(Long importId) {
        this.importId = importId;
    }

    public String getImportChannelId() {
        return importChannelId;
    }

    public void setImportChannelId(String importChannelId) {
        this.importChannelId = importChannelId;
    }

    public List<Attachment> getAttachments() {
        return attachments;
    }

    public void setAttachments(List<Attachment> attachments) {
        this.attachments = attachments;
    }

    public Map<Integer, ConnectorMessage> getConnectorMessages() {
        return connectorMessages;
    }

    public ConnectorMessage getMergedConnectorMessage() {
        if (mergedConnectorMessage == null) {
            mergedConnectorMessage = new ConnectorMessage();
            mergedConnectorMessage.setChannelId(channelId);
            mergedConnectorMessage.setMessageId(messageId);
            mergedConnectorMessage.setServerId(serverId);
            mergedConnectorMessage.setReceivedDate(receivedDate);

            Map<String, Object> sourceMap = null;
            Map<String, Object> responseMap = new HashMap<String, Object>();
            Map<String, Object> channelMap = new HashMap<String, Object>();

            ConnectorMessage sourceConnectorMessage = connectorMessages.get(0);

            if (sourceConnectorMessage != null) {
                mergedConnectorMessage.setRaw(sourceConnectorMessage.getRaw());
                mergedConnectorMessage.setProcessedRaw(sourceConnectorMessage.getProcessedRaw());
                sourceMap = sourceConnectorMessage.getSourceMap();
                responseMap.putAll(sourceConnectorMessage.getResponseMap());
                channelMap.putAll(sourceConnectorMessage.getChannelMap());
            }

            List<ConnectorMessage> orderedConnectorMessages = new ArrayList<ConnectorMessage>(connectorMessages.values());
            Collections.sort(orderedConnectorMessages, new Comparator<ConnectorMessage>() {
                @Override
                public int compare(ConnectorMessage m1, ConnectorMessage m2) {
                    if (m1.getChainId() == m2.getChainId()) {
                        return m1.getOrderId() - m2.getOrderId();
                    } else {
                        return m1.getChainId() - m2.getChainId();
                    }
                }
            });

            for (ConnectorMessage connectorMessage : orderedConnectorMessages) {
                if (connectorMessage.getMetaDataId() > 0) {
                    if (sourceMap == null) {
                        sourceMap = connectorMessage.getSourceMap();
                    }
                    responseMap.putAll(connectorMessage.getResponseMap());
                    channelMap.putAll(connectorMessage.getChannelMap());
                }
            }

            mergedConnectorMessage.setSourceMap(sourceMap);
            mergedConnectorMessage.setResponseMap(responseMap);
            mergedConnectorMessage.setChannelMap(channelMap);
        }

        return mergedConnectorMessage;
    }

    public String toString() {
        return "message " + messageId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((attachments == null) ? 0 : attachments.hashCode());
        result = prime * result
                + ((channelId == null) ? 0 : channelId.hashCode());
        result = prime * result + ((connectorMessages == null) ? 0
                : connectorMessages.hashCode());
        result = prime * result
                + ((importChannelId == null) ? 0 : importChannelId.hashCode());
        result = prime * result
                + ((importId == null) ? 0 : importId.hashCode());
        result = prime * result
                + ((messageId == null) ? 0 : messageId.hashCode());
        result = prime * result
                + ((originalId == null) ? 0 : originalId.hashCode());
        result = prime * result + (processed ? 1231 : 1237);
        result = prime * result
                + ((receivedDate == null) ? 0 : receivedDate.hashCode());
        result = prime * result
                + ((serverId == null) ? 0 : serverId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
//        if (getClass() != obj.getClass()) {
//            return false;
//        }
        Message other = (Message) obj;
        if (attachments == null) {
            if (other.attachments != null) {
                return false;
            }
        } else if (!attachments.equals(other.attachments)) {
            return false;
        }
        if (channelId == null) {
            if (other.channelId != null) {
                return false;
            }
        } else if (!channelId.equals(other.channelId)) {
            return false;
        }
        if (connectorMessages == null) {
            if (other.connectorMessages != null) {
                return false;
            }
        } else if (!connectorMessages.equals(other.connectorMessages)) {
            return false;
        }
        if (importChannelId == null) {
            if (other.importChannelId != null) {
                return false;
            }
        } else if (!importChannelId.equals(other.importChannelId)) {
            return false;
        }
        if (importId == null) {
            if (other.importId != null) {
                return false;
            }
        } else if (!importId.equals(other.importId)) {
            return false;
        }
        if (messageId == null) {
            if (other.messageId != null) {
                return false;
            }
        } else if (!messageId.equals(other.messageId)) {
            return false;
        }
        if (originalId == null) {
            if (other.originalId != null) {
                return false;
            }
        } else if (!originalId.equals(other.originalId)) {
            return false;
        }
        if (processed != other.processed) {
            return false;
        }
        if (receivedDate == null) {
            if (other.receivedDate != null) {
                return false;
            }
        } else if (!receivedDate.equals(other.receivedDate)) {
            return false;
        }
        if (serverId == null) {
            if (other.serverId != null) {
                return false;
            }
        } else if (!serverId.equals(other.serverId)) {
            return false;
        }
        return true;
    }
}
