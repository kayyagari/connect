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
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("connectorMessage")
public class ConnectorMessage implements Serializable {
    protected long messageId;
    protected int metaDataId;
    protected String channelId;
    protected String channelName;
    protected String connectorName;
    protected String serverId;
    protected Calendar receivedDate;
    protected Status status;
    private MessageContent raw;
    private MessageContent processedRaw;
    private MessageContent transformed;
    private MessageContent encoded;
    private MessageContent sent;
    private MessageContent response;
    private MessageContent responseTransformed;
    private MessageContent processedResponse;
    protected MapContent sourceMapContent = new MapContent();
    protected MapContent connectorMapContent = new MapContent();
    protected MapContent channelMapContent = new MapContent();
    protected MapContent responseMapContent = new MapContent();
    protected Map<String, Object> metaDataMap = new HashMap<String, Object>();
    protected ErrorContent processingErrorContent = new ErrorContent();
    protected ErrorContent postProcessorErrorContent = new ErrorContent();
    protected ErrorContent responseErrorContent = new ErrorContent();
    protected int errorCode = 0;
    protected int sendAttempts = 0;
    protected Calendar sendDate;
    protected Calendar responseDate;
    protected int chainId;
    protected int orderId;

    protected transient ConnectorProperties sentProperties;
    protected transient Integer queueBucket;
    protected transient boolean attemptedFirst;
    protected transient long dispatcherId;

    public ConnectorMessage() {}

    public ConnectorMessage(String channelId, String channelName, long messageId, int metaDataId, String serverId, Calendar receivedDate, Status status) {
        this.channelId = channelId;
        this.channelName = channelName;
        this.messageId = messageId;
        this.metaDataId = metaDataId;
        this.serverId = serverId;
        this.receivedDate = receivedDate;
        this.status = status;
    }

    public int getMetaDataId() {
        return metaDataId;
    }

    public void setMetaDataId(int metaDataId) {
        this.metaDataId = metaDataId;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public String getChannelName() {
        return channelName;
    }

    public void setChannelName(String channelName) {
        this.channelName = channelName;
    }

    public String getConnectorName() {
        return connectorName;
    }

    public void setConnectorName(String connectorName) {
        this.connectorName = connectorName;
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public Calendar getReceivedDate() {
        return receivedDate;
    }

    public void setReceivedDate(Calendar receivedDate) {
        this.receivedDate = receivedDate;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public MessageContent getMessageContent(ContentType contentType) {
        switch (contentType) {
            case RAW:
                return raw;
            case PROCESSED_RAW:
                return processedRaw;
            case TRANSFORMED:
                return transformed;
            case ENCODED:
                return encoded;
            case SENT:
                return sent;
            case RESPONSE:
                return response;
            case RESPONSE_TRANSFORMED:
                return responseTransformed;
            case PROCESSED_RESPONSE:
                return processedResponse;
            default:
                return null;
        }
    }

    public void setMessageContent(MessageContent messageContent) {
        switch (messageContent.getContentType()) {
            case RAW:
                setRaw(messageContent);
                break;
            case PROCESSED_RAW:
                setProcessedRaw(messageContent);
                break;
            case TRANSFORMED:
                setTransformed(messageContent);
                break;
            case ENCODED:
                setEncoded(messageContent);
                break;
            case SENT:
                setSent(messageContent);
                break;
            case RESPONSE:
                setResponse(messageContent);
                break;
            case RESPONSE_TRANSFORMED:
                setResponseTransformed(messageContent);
                break;
            case PROCESSED_RESPONSE:
                setProcessedResponse(messageContent);
                break;
            default:
                /*
                 * if the content type is not recognized, then this code needs to be fixed to
                 * include all possible content types. We throw a runtime exception since this is an
                 * internal error that needs to be corrected.
                 */
                throw new RuntimeException("Unrecognized content type: " + messageContent.getContentType().getContentTypeCode());
        }
    }

    public MessageContent getRaw() {
        return raw;
    }

    public void setRaw(MessageContent messageContentRaw) {
        this.raw = messageContentRaw;
    }

    public MessageContent getProcessedRaw() {
        return processedRaw;
    }

    public void setProcessedRaw(MessageContent processedRaw) {
        this.processedRaw = processedRaw;
    }

    public MessageContent getTransformed() {
        return transformed;
    }

    public void setTransformed(MessageContent messageContentTransformed) {
        this.transformed = messageContentTransformed;
    }

    public MessageContent getEncoded() {
        return encoded;
    }

    public void setEncoded(MessageContent messageContentEncoded) {
        this.encoded = messageContentEncoded;
    }

    public MessageContent getSent() {
        return sent;
    }

    public void setSent(MessageContent messageContentSent) {
        this.sent = messageContentSent;
    }

    public MessageContent getResponse() {
        return response;
    }

    public void setResponse(MessageContent messageContentResponse) {
        this.response = messageContentResponse;
    }

    public MessageContent getResponseTransformed() {
        return responseTransformed;
    }

    public void setResponseTransformed(MessageContent responseTransformed) {
        this.responseTransformed = responseTransformed;
    }

    public MessageContent getProcessedResponse() {
        return processedResponse;
    }

    public void setProcessedResponse(MessageContent processedResponse) {
        this.processedResponse = processedResponse;
    }

    public long getMessageId() {
        return messageId;
    }

    public void setMessageId(long messageId) {
        this.messageId = messageId;
    }

    public MapContent getSourceMapContent() {
        return sourceMapContent;
    }

    public void setSourceMapContent(MapContent sourceMapContent) {
        this.sourceMapContent = sourceMapContent;
    }

    public MapContent getConnectorMapContent() {
        return connectorMapContent;
    }

    public void setConnectorMapContent(MapContent connectorMapContent) {
        this.connectorMapContent = connectorMapContent;
    }

    public MapContent getChannelMapContent() {
        return channelMapContent;
    }

    public void setChannelMapContent(MapContent channelMapContent) {
        this.channelMapContent = channelMapContent;
    }

    public MapContent getResponseMapContent() {
        return responseMapContent;
    }

    public void setResponseMapContent(MapContent responseMapContent) {
        this.responseMapContent = responseMapContent;
    }

    public Map<String, Object> getSourceMap() {
        return sourceMapContent.getMap();
    }

    public void setSourceMap(Map<String, Object> sourceMap) {
        sourceMapContent.setMap(sourceMap);
    }

    public Map<String, Object> getConnectorMap() {
        return connectorMapContent.getMap();
    }

    public void setConnectorMap(Map<String, Object> connectorMap) {
        connectorMapContent.setMap(connectorMap);
    }

    public Map<String, Object> getChannelMap() {
        return channelMapContent.getMap();
    }

    public void setChannelMap(Map<String, Object> channelMap) {
        channelMapContent.setMap(channelMap);
    }

    public Map<String, Object> getResponseMap() {
        return responseMapContent.getMap();
    }

    public void setResponseMap(Map<String, Object> responseMap) {
        responseMapContent.setMap(responseMap);
    }

    public Map<String, Object> getMetaDataMap() {
        return metaDataMap;
    }

    public void setMetaDataMap(Map<String, Object> metaDataMap) {
        this.metaDataMap = metaDataMap;
    }

    public ErrorContent getProcessingErrorContent() {
        return processingErrorContent;
    }

    public void setProcessingErrorContent(ErrorContent processingErrorContent) {
        this.processingErrorContent = processingErrorContent;
    }

    public ErrorContent getPostProcessorErrorContent() {
        return postProcessorErrorContent;
    }

    public void setPostProcessorErrorContent(ErrorContent postProcessorErrorContent) {
        this.postProcessorErrorContent = postProcessorErrorContent;
    }

    public ErrorContent getResponseErrorContent() {
        return responseErrorContent;
    }

    public void setResponseErrorContent(ErrorContent responseErrorContent) {
        this.responseErrorContent = responseErrorContent;
    }

    public String getProcessingError() {
        return processingErrorContent.getContent();
    }

    public void setProcessingError(String processingError) {
        processingErrorContent.setContent(processingError);

        updateErrorCode();
    }

    public String getPostProcessorError() {
        return postProcessorErrorContent.getContent();
    }

    public void setPostProcessorError(String postProcessorError) {
        postProcessorErrorContent.setContent(postProcessorError);

        updateErrorCode();
    }

    public String getResponseError() {
        return responseErrorContent.getContent();
    }

    public void setResponseError(String responseError) {
        responseErrorContent.setContent(responseError);

        updateErrorCode();
    }

    /**
     * Returns whether the connectorMessage contains an error of the content type that is provided.
     * The connector error code is the sum of all individual error codes of all the errors that
     * exist. Since individual error codes are all powers of 2, we can use bitwise operators to
     * determine the existence of an individual error.
     */
    public boolean containsError(ContentType contentType) {
        int errorCode = contentType.getErrorCode();

        if (errorCode > 0) {
            return (this.errorCode & errorCode) == errorCode;
        }

        return false;
    }

    /**
     * Update the errorCode of the connector message.
     */
    private void updateErrorCode() {
        // The errorCode is the sum of all the individual error codes for which an error exists.
        errorCode = 0;

        if (getProcessingError() != null) {
            errorCode += ContentType.PROCESSING_ERROR.getErrorCode();
        }
        if (getPostProcessorError() != null) {
            errorCode += ContentType.POSTPROCESSOR_ERROR.getErrorCode();
        }
        if (getResponseError() != null) {
            errorCode += ContentType.RESPONSE_ERROR.getErrorCode();
        }
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public int getSendAttempts() {
        return sendAttempts;
    }

    public void setSendAttempts(int sendAttempts) {
        this.sendAttempts = sendAttempts;
    }

    public Calendar getSendDate() {
        return sendDate;
    }

    public void setSendDate(Calendar sendDate) {
        this.sendDate = sendDate;
    }

    public Calendar getResponseDate() {
        return responseDate;
    }

    public void setResponseDate(Calendar responseDate) {
        this.responseDate = responseDate;
    }

    public int getChainId() {
        return chainId;
    }

    public void setChainId(int chainId) {
        this.chainId = chainId;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public ConnectorProperties getSentProperties() {
        return sentProperties;
    }

    public void setSentProperties(ConnectorProperties sentProperties) {
        this.sentProperties = sentProperties;
    }

    public Integer getQueueBucket() {
        return queueBucket;
    }

    public void setQueueBucket(Integer queueBucket) {
        this.queueBucket = queueBucket;
    }

    public boolean isAttemptedFirst() {
        return attemptedFirst;
    }

    public void setAttemptedFirst(boolean attemptedFirst) {
        this.attemptedFirst = attemptedFirst;
    }

    public long getDispatcherId() {
        return dispatcherId;
    }

    public void setDispatcherId(long dispatcherId) {
        this.dispatcherId = dispatcherId;
    }

    public String toString() {
        return "message " + messageId + "-" + metaDataId + " (" + status + ")";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + chainId;
        result = prime * result
                + ((channelId == null) ? 0 : channelId.hashCode());
        result = prime * result + ((channelMapContent == null) ? 0
                : channelMapContent.hashCode());
        result = prime * result
                + ((channelName == null) ? 0 : channelName.hashCode());
        result = prime * result + ((connectorMapContent == null) ? 0
                : connectorMapContent.hashCode());
        result = prime * result
                + ((connectorName == null) ? 0 : connectorName.hashCode());
        result = prime * result + ((encoded == null) ? 0 : encoded.hashCode());
        result = prime * result + errorCode;
        result = prime * result + (int) (messageId ^ (messageId >>> 32));
        result = prime * result + metaDataId;
        result = prime * result
                + ((metaDataMap == null) ? 0 : metaDataMap.hashCode());
        result = prime * result + orderId;
        result = prime * result + ((postProcessorErrorContent == null) ? 0
                : postProcessorErrorContent.hashCode());
        result = prime * result
                + ((processedRaw == null) ? 0 : processedRaw.hashCode());
        result = prime * result + ((processedResponse == null) ? 0
                : processedResponse.hashCode());
        result = prime * result + ((processingErrorContent == null) ? 0
                : processingErrorContent.hashCode());
        result = prime * result + ((raw == null) ? 0 : raw.hashCode());
        result = prime * result
                + ((receivedDate == null) ? 0 : receivedDate.hashCode());
        result = prime * result
                + ((response == null) ? 0 : response.hashCode());
        result = prime * result
                + ((responseDate == null) ? 0 : responseDate.hashCode());
        result = prime * result + ((responseErrorContent == null) ? 0
                : responseErrorContent.hashCode());
        result = prime * result + ((responseMapContent == null) ? 0
                : responseMapContent.hashCode());
        result = prime * result + ((responseTransformed == null) ? 0
                : responseTransformed.hashCode());
        result = prime * result + sendAttempts;
        result = prime * result
                + ((sendDate == null) ? 0 : sendDate.hashCode());
        result = prime * result + ((sent == null) ? 0 : sent.hashCode());
        result = prime * result
                + ((serverId == null) ? 0 : serverId.hashCode());
        result = prime * result + ((sourceMapContent == null) ? 0
                : sourceMapContent.hashCode());
        result = prime * result + ((status == null) ? 0 : status.hashCode());
        result = prime * result
                + ((transformed == null) ? 0 : transformed.hashCode());
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
        ConnectorMessage other = (ConnectorMessage) obj;
        if (chainId != other.chainId) {
            return false;
        }
        if (channelId == null) {
            if (other.channelId != null) {
                return false;
            }
        } else if (!channelId.equals(other.channelId)) {
            return false;
        }
        if (channelMapContent == null) {
            if (other.channelMapContent != null) {
                return false;
            }
        } else if (!channelMapContent.equals(other.channelMapContent)) {
            return false;
        }
        if (channelName == null) {
            if (other.channelName != null) {
                return false;
            }
        } else if (!channelName.equals(other.channelName)) {
            return false;
        }
        if (connectorMapContent == null) {
            if (other.connectorMapContent != null) {
                return false;
            }
        } else if (!connectorMapContent.equals(other.connectorMapContent)) {
            return false;
        }
        if (connectorName == null) {
            if (other.connectorName != null) {
                return false;
            }
        } else if (!connectorName.equals(other.connectorName)) {
            return false;
        }
        if (encoded == null) {
            if (other.encoded != null) {
                return false;
            }
        } else if (!encoded.equals(other.encoded)) {
            return false;
        }
        if (errorCode != other.errorCode) {
            return false;
        }
        if (messageId != other.messageId) {
            return false;
        }
        if (metaDataId != other.metaDataId) {
            return false;
        }
        if (metaDataMap == null) {
            if (other.metaDataMap != null) {
                return false;
            }
        } else if (!metaDataMap.equals(other.metaDataMap)) {
            return false;
        }
        if (orderId != other.orderId) {
            return false;
        }
        if (postProcessorErrorContent == null) {
            if (other.postProcessorErrorContent != null) {
                return false;
            }
        } else if (!postProcessorErrorContent
                .equals(other.postProcessorErrorContent)) {
            return false;
        }
        if (processedRaw == null) {
            if (other.processedRaw != null) {
                return false;
            }
        } else if (!processedRaw.equals(other.processedRaw)) {
            return false;
        }
        if (processedResponse == null) {
            if (other.processedResponse != null) {
                return false;
            }
        } else if (!processedResponse.equals(other.processedResponse)) {
            return false;
        }
        if (processingErrorContent == null) {
            if (other.processingErrorContent != null) {
                return false;
            }
        } else if (!processingErrorContent
                .equals(other.processingErrorContent)) {
            return false;
        }
        if (raw == null) {
            if (other.raw != null) {
                return false;
            }
        } else if (!raw.equals(other.raw)) {
            return false;
        }
        if (receivedDate == null) {
            if (other.receivedDate != null) {
                return false;
            }
        } else if (!receivedDate.equals(other.receivedDate)) {
            return false;
        }
        if (response == null) {
            if (other.response != null) {
                return false;
            }
        } else if (!response.equals(other.response)) {
            return false;
        }
        if (responseDate == null) {
            if (other.responseDate != null) {
                return false;
            }
        } else if (!responseDate.equals(other.responseDate)) {
            return false;
        }
        if (responseErrorContent == null) {
            if (other.responseErrorContent != null) {
                return false;
            }
        } else if (!responseErrorContent.equals(other.responseErrorContent)) {
            return false;
        }
        if (responseMapContent == null) {
            if (other.responseMapContent != null) {
                return false;
            }
        } else if (!responseMapContent.equals(other.responseMapContent)) {
            return false;
        }
        if (responseTransformed == null) {
            if (other.responseTransformed != null) {
                return false;
            }
        } else if (!responseTransformed.equals(other.responseTransformed)) {
            return false;
        }
        if (sendAttempts != other.sendAttempts) {
            return false;
        }
        if (sendDate == null) {
            if (other.sendDate != null) {
                return false;
            }
        } else if (!sendDate.equals(other.sendDate)) {
            return false;
        }
        if (sent == null) {
            if (other.sent != null) {
                return false;
            }
        } else if (!sent.equals(other.sent)) {
            return false;
        }
        if (serverId == null) {
            if (other.serverId != null) {
                return false;
            }
        } else if (!serverId.equals(other.serverId)) {
            return false;
        }
        if (sourceMapContent == null) {
            if (other.sourceMapContent != null) {
                return false;
            }
        } else if (!sourceMapContent.equals(other.sourceMapContent)) {
            return false;
        }
        if (status != other.status) {
            return false;
        }
        if (transformed == null) {
            if (other.transformed != null) {
                return false;
            }
        } else if (!transformed.equals(other.transformed)) {
            return false;
        }
        return true;
    }
}
