package com.mirth.connect.connectors.core;

import com.mirth.connect.donkey.model.channel.ConnectorProperties;

public interface ConnectorSettingsPanelPlugin {
	
	void initialize(ConnectorSettingsPanelBase connectorBase);

	ConnectorProperties getProperties();
	
	void setProperties(ConnectorProperties properties);
	
	ConnectorProperties getDefaults();
	
	boolean checkProperties(ConnectorProperties properties, boolean highlight);
	
	void resetInvalidProperties();
	
	String getConnectorName();
	
	String getRequiredInboundDataType();
	
	String getInitialOutboundDataType();
	
	String getInitialInboundResponseDataType();
	
	String getInitialOutboundResponseDataType();
	
	void initComponents();
	
	void initLayout();
	
}
