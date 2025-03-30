package com.mirth.connect.donkey.server.channel;

import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.donkey.model.message.ConnectorMessage;
import com.mirth.connect.donkey.model.message.Response;
import com.mirth.connect.donkey.server.ConnectorTaskException;

public interface DestinationConnectorPlugin {
	
	public void initialize(IDestinationConnector connector);

    public void replaceConnectorProperties(ConnectorProperties connectorProperties, ConnectorMessage message);

    public Response send(ConnectorProperties connectorProperties, ConnectorMessage message);
    
    public void onDeploy() throws ConnectorTaskException;
    
    public void onUndeploy() throws ConnectorTaskException;

    public void onStart() throws ConnectorTaskException;

    public void onStop() throws ConnectorTaskException;

    public void onHalt() throws ConnectorTaskException;
    
    default public String getConfigurationClass() {
    	return null;
    }
    
}
