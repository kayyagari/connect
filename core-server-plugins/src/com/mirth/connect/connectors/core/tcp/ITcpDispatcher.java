package com.mirth.connect.connectors.core.tcp;

import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.donkey.model.message.ConnectorMessage;
import com.mirth.connect.donkey.model.message.Response;
import com.mirth.connect.donkey.server.ConnectorTaskException;
import com.mirth.connect.donkey.server.channel.IDestinationConnector;

public interface ITcpDispatcher extends IDestinationConnector {

    public Response doSend(ConnectorProperties connectorProperties, ConnectorMessage message);
    
    public void doOnDeploy() throws ConnectorTaskException;
    
    public void doOnUndeploy() throws ConnectorTaskException;

    public void doOnStart() throws ConnectorTaskException;

    public void doOnStop() throws ConnectorTaskException;

    public void doOnHalt() throws ConnectorTaskException;
    
}
