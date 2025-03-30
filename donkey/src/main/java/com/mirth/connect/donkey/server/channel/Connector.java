/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.donkey.server.channel;

import java.util.Map;
import java.util.Set;

import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.donkey.model.channel.DebugOptions;
import com.mirth.connect.donkey.model.channel.DeployedState;
import com.mirth.connect.donkey.model.message.DataType;
import com.mirth.connect.donkey.server.ConnectorTaskException;

public abstract class Connector implements IConnector {
    protected Channel channel;

    private String channelId;
    private int metaDataId;
    private DataType inboundDataType;
    private DataType outboundDataType;
    private DeployedState currentState = DeployedState.STOPPED;
    private ConnectorProperties connectorProperties;
    private Map<String, Integer> destinationIdMap;
    private FilterTransformerExecutor filterTransformerExecutor;
    private Set<String> resourceIds;

    public abstract void onDeploy() throws ConnectorTaskException;
    
    public abstract void onUndeploy() throws ConnectorTaskException;

    public abstract void onStart() throws ConnectorTaskException;

    public abstract void onStop() throws ConnectorTaskException;

    public abstract void onHalt() throws ConnectorTaskException;

    public abstract void start() throws ConnectorTaskException, InterruptedException;

    public abstract void stop() throws ConnectorTaskException, InterruptedException;

    public abstract void halt() throws ConnectorTaskException, InterruptedException;
    
    public void onDebugDeploy(DebugOptions debugOptions) throws ConnectorTaskException {
    	onDeploy();
    }
    
    public void stopDebugging() throws ConnectorTaskException {

    }

    @Override
    public Channel getChannel() {
        return channel;
    }

    @Override
    public void setChannel(IChannel channel) {
        this.channel = (Channel) channel;
    }

    @Override
    public String getChannelId() {
        return channelId;
    }

    @Override
    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }
    
    @Override
    public String getChannelName() {
        return channel != null ? channel.getName() : null;
    }

    @Override
    public int getMetaDataId() {
        return metaDataId;
    }

    @Override
    public void setMetaDataId(int metaDataId) {
        this.metaDataId = metaDataId;
    }

    @Override
    public DataType getInboundDataType() {
        return inboundDataType;
    }

    @Override
    public void setInboundDataType(DataType inboundDataType) {
        this.inboundDataType = inboundDataType;
    }

    @Override
    public DataType getOutboundDataType() {
        return outboundDataType;
    }

    @Override
    public void setOutboundDataType(DataType outboundDataType) {
        this.outboundDataType = outboundDataType;
    }

    @Override
    public DeployedState getCurrentState() {
        return currentState;
    }

    @Override
    public void setCurrentState(DeployedState currentState) {
        this.currentState = currentState;
    }

    @Override
    public ConnectorProperties getConnectorProperties() {
        return connectorProperties;
    }

    @Override
    public void setConnectorProperties(ConnectorProperties connectorProperties) {
        this.connectorProperties = connectorProperties;
    }

    @Override
    public Map<String, Integer> getDestinationIdMap() {
        return destinationIdMap;
    }

    @Override
    public void setDestinationIdMap(Map<String, Integer> destinationIdMap) {
        this.destinationIdMap = destinationIdMap;
    }

    @Override
    public FilterTransformerExecutor getFilterTransformerExecutor() {
        return filterTransformerExecutor;
    }

    @Override
    public void setFilterTransformerExecutor(Object filterTransformerExecutor) {
        this.filterTransformerExecutor = (FilterTransformerExecutor) filterTransformerExecutor;
    }

    @Override
    public Set<String> getResourceIds() {
        return resourceIds;
    }

    @Override
    public void setResourceIds(Set<String> resourceIds) {
        this.resourceIds = resourceIds;
    }

    @Override
    public String getConfigurationClass() {
        return null;
    }
}
