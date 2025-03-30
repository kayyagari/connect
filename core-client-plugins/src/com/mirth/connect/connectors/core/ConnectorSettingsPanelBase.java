package com.mirth.connect.connectors.core;

import com.mirth.connect.donkey.model.channel.ConnectorProperties;

public interface ConnectorSettingsPanelBase {

	/***
	 * 
	 * @param connectorPlugin
	 * 
	 * Initialize the base ConnectorSettingsPanel object. All initialization logic, such as calling initComponents()
	 * and initLayout(), should go in this method instead of the constructor.
	 */
	void initialize(ConnectorSettingsPanelPlugin connectorPlugin);
	
	void doSetProperties(final ConnectorProperties properties);
	
	ConnectorProperties doGetProperties();
	
	boolean doCheckProperties(ConnectorProperties properties, boolean highlight);
	
	void doResetInvalidProperties();
	
	ConnectorProperties getFilledProperties();
	
}
