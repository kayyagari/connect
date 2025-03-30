package com.mirth.connect.connectors.core;

import com.mirth.connect.client.ui.ConnectorTypeDecoration;
import com.mirth.connect.connectors.core.ws.IWebServiceDispatcherProperties;

public interface InteropConnectorSettingsPanelPlugin extends ConnectorSettingsPanelPlugin {
	
	void generateEnvelope();

	ConnectorTypeDecoration getConnectorTypeDecoration();
	
	void doLocalDecoration(ConnectorTypeDecoration connectorTypeDecoration);
	
	void setSoapEnvelopeText(String text);
	
	boolean canSetLocationURI();
	
	void initToolTips();
	
	boolean canTestConnection(boolean wsdlUrl);
	
	IWebServiceDispatcherProperties getTestConnectionPropeties();
	
}
