package com.mirth.connect.connectors.core.interop;

import java.net.URL;

import javax.xml.soap.SOAPMessage;

import com.mirth.connect.connectors.core.ws.IDispatchContainer;
import com.mirth.connect.connectors.core.ws.IWebServiceDispatcherProperties;
import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.donkey.model.channel.DeployedState;
import com.mirth.connect.donkey.model.message.ConnectorMessage;
import com.mirth.connect.donkey.model.message.Status;
import com.mirth.connect.donkey.server.ConnectorTaskException;
import com.mirth.connect.donkey.server.channel.DestinationConnectorPlugin;

public interface InteropDispatcherPlugin extends DestinationConnectorPlugin {

	URL getWsdlUrl(IWebServiceDispatcherProperties webServiceDispatcherProperties, IDispatchContainer dispatchContainer, int timeout) throws Exception;
	
	void handleSOAPResult(ConnectorProperties connectorProperties, ConnectorMessage connectorMessage, SOAPMessage result) throws Exception;
	
	String getConfigurationClass();
	
	void start() throws ConnectorTaskException, InterruptedException;
	
	void startQueue();
	
	void stop() throws ConnectorTaskException, InterruptedException;
	
	void halt() throws ConnectorTaskException, InterruptedException;
	
	void setForceQueue(boolean forceQueue);
	
	DeployedState getCurrentState();
	
	void updateCurrentState(DeployedState currentState);
	
	void transform(Object dao, ConnectorMessage message, Status previousStatus, boolean initialAttempt) throws InterruptedException;
	
	void process(Object dao, ConnectorMessage message, Status previousStatus) throws InterruptedException;
	
	void updateQueuedStatus(Object dao, ConnectorMessage message, Status previousStatus) throws InterruptedException;
	
	void processPendingConnectorMessage(Object dao, ConnectorMessage message) throws InterruptedException;
	
	void run();
	
}
