package com.mirth.connect.donkey.server.channel;

import java.util.Map;
import java.util.Set;

import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.donkey.model.channel.DeployedState;
import com.mirth.connect.donkey.model.message.DataType;

public interface IConnector {
	
	public IChannel getChannel();
	
	public void setChannel(IChannel channel);

    public String getChannelId();
    
    public void setChannelId(String channelId);
    
    public String getChannelName();
    
    public String getConnectorName();
    
    public int getMetaDataId();
    
    public void setMetaDataId(int metaDataId);
    
    public DataType getInboundDataType();
    
    public void setInboundDataType(DataType inboundDataType);
    
    public DataType getOutboundDataType();
    
    public void setOutboundDataType(DataType outboundDataType);
    
    public DeployedState getCurrentState();
    
    public void setCurrentState(DeployedState currentState);
    
    public ConnectorProperties getConnectorProperties();
    
    public void setConnectorProperties(ConnectorProperties connectorProperties);
    
    public Map<String, Integer> getDestinationIdMap();

    public void setDestinationIdMap(Map<String, Integer> destinationIdMap);
    
    public Object getFilterTransformerExecutor();
    
    public void setFilterTransformerExecutor(Object filterTransformerExecutor);

    public Set<String> getResourceIds();
    
    public void setResourceIds(Set<String> resourceIds);

    public String getConfigurationClass();
    
}
