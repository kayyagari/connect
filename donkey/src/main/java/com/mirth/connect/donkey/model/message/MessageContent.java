/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.donkey.model.message;

public class MessageContent extends Content {
    private String channelId;
    private long messageId;
    private int metaDataId;
    private ContentType contentType;
    private String content;
    private String dataType;

    public MessageContent() {}

    public MessageContent(String channelId, long messageId, int metaDataId, ContentType contentType, String content, String dataType, boolean encrypted) {
        this.channelId = channelId;
        this.messageId = messageId;
        this.metaDataId = metaDataId;
        this.contentType = contentType;
        this.content = content;
        this.dataType = dataType;
        super.setEncrypted(encrypted);
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public ContentType getContentType() {
        return contentType;
    }

    public void setContentType(ContentType contentType) {
        this.contentType = contentType;
    }

    @Override
    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public long getMessageId() {
        return messageId;
    }

    public void setMessageId(Long messageId) {
        this.messageId = messageId;
    }

    public int getMetaDataId() {
        return metaDataId;
    }

    public void setMetaDataId(Integer metaDataId) {
        this.metaDataId = metaDataId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result
                + ((channelId == null) ? 0 : channelId.hashCode());
        result = prime * result + ((content == null) ? 0 : content.hashCode());
        result = prime * result
                + ((contentType == null) ? 0 : contentType.hashCode());
        result = prime * result
                + ((dataType == null) ? 0 : dataType.hashCode());
        result = prime * result + (int) (messageId ^ (messageId >>> 32));
        result = prime * result + metaDataId;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
//        if (getClass() != obj.getClass()) {
//            return false;
//        }
        MessageContent other = (MessageContent) obj;
        if (channelId == null) {
            if (other.channelId != null) {
                return false;
            }
        } else if (!channelId.equals(other.channelId)) {
            return false;
        }
        if (content == null) {
            if (other.content != null) {
                return false;
            }
        } else if (!content.equals(other.content)) {
            return false;
        }
        if (contentType != other.contentType) {
            return false;
        }
        if (dataType == null) {
            if (other.dataType != null) {
                return false;
            }
        } else if (!dataType.equals(other.dataType)) {
            return false;
        }
        if (messageId != other.messageId) {
            return false;
        }
        if (metaDataId != other.metaDataId) {
            return false;
        }
        return true;
    }
}
