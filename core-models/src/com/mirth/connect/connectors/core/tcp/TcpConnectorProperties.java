package com.mirth.connect.connectors.core.tcp;

import java.util.Set;

import com.mirth.connect.donkey.model.channel.ConnectorPluginProperties;

public interface TcpConnectorProperties {
	
	public Set<ConnectorPluginProperties> getPluginProperties() ;
	
	public void setPluginProperties(Set<ConnectorPluginProperties> pluginProperties);
	
	public String getRemoteAddress();
	
	public void setRemoteAddress(String remoteAddress);
	
	public String getRemotePort();
	
	public void setRemotePort(String remotePort);
	
	public boolean isOverrideLocalBinding();
	
	public void setOverrideLocalBinding(boolean overrideLocalBinding);
	
	public boolean isServerMode();
	
	public void setServerMode(boolean serverMode);
}
