package com.mirth.connect.connectors.core.tcp;

import com.mirth.connect.donkey.server.ConnectorTaskException;
import com.mirth.connect.donkey.server.channel.DispatchResult;
import com.mirth.connect.donkey.server.channel.ISourceConnector;

public interface ITcpReceiver extends ISourceConnector {
	
	public void doHandleRecoveredResponse(DispatchResult dispatchResult);

    public void doOnDeploy() throws ConnectorTaskException;
    
    public void doOnUndeploy() throws ConnectorTaskException;

    public void doOnStart() throws ConnectorTaskException;

    public void doOnStop() throws ConnectorTaskException;

    public void doOnHalt() throws ConnectorTaskException;
    
}
