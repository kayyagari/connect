package com.mirth.connect.donkey.server.channel;

import com.mirth.connect.donkey.server.ConnectorTaskException;

public interface SourceConnectorPlugin {
	
	public void initialize(ISourceConnector connector);
	
	public void handleRecoveredResponse(DispatchResult dispatchResult);

    public void onDeploy() throws ConnectorTaskException;
    
    public void onUndeploy() throws ConnectorTaskException;

    public void onStart() throws ConnectorTaskException;

    public void onStop() throws ConnectorTaskException;

    public void onHalt() throws ConnectorTaskException;
    
    default public String getConfigurationClass() {
    	return null;
    }
    
}
