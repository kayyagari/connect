package com.mirth.connect.connectors.core.tcp;

import java.util.Set;

import com.mirth.connect.donkey.model.channel.ConnectorPluginProperties;
import com.mirth.connect.donkey.model.channel.ListenerConnectorProperties;
import com.mirth.connect.donkey.model.channel.SourceConnectorProperties;

public interface ITcpReceiverProperties extends TcpConnectorProperties {
	
    public static final int SAME_CONNECTION = 0;
    public static final int NEW_CONNECTION = 1;
    public static final int NEW_CONNECTION_ON_RECOVERY = 2;
	
	public int getRespondOnNewConnection();
	
	public String getResponseAddress();
	
	public Set<ConnectorPluginProperties> getResponseConnectorPluginProperties();
	
	public void setListenerConnectorProperties(ListenerConnectorProperties listenerConnectorProperties);
	
	public void setSourceConnectorProperties(SourceConnectorProperties sourceConnectorProperties);
	
    public Set<ConnectorPluginProperties> getPluginProperties();
    
    public void setPluginProperties(Set<ConnectorPluginProperties> pluginProperties);
    
    public String getReceiveTimeout();
    
    public void setReceiveTimeout(String receiveTimeout);
	
}
