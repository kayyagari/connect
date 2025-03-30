package com.mirth.connect.connectors.core.interop;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import com.mirth.connect.connectors.core.http.HttpRequestMessage;
import com.mirth.connect.connectors.core.http.IRequest;
import com.mirth.connect.donkey.model.channel.DeployedState;
import com.mirth.connect.donkey.model.message.attachment.Attachment;
import com.mirth.connect.donkey.server.ConnectorTaskException;
import com.mirth.connect.donkey.server.channel.DispatchResult;
import com.mirth.connect.donkey.server.channel.SourceConnectorPlugin;

public interface InteropReceiverPlugin extends SourceConnectorPlugin {

	Object getMessage(IRequest request, Map<String, Object> sourceMap, List<Attachment> attachments) throws Exception;
	
	void populateSourceMap(IRequest request, HttpRequestMessage requestMessage, Map<String, Object> sourceMap);
	
	public void sendResponse(IRequest baseRequest, HttpServletResponse servletResponse, DispatchResult dispatchResult) throws Exception;
	
	public void sendErrorResponse(IRequest baseRequest, HttpServletResponse servletResponse, DispatchResult dispatchResult, Throwable t) throws IOException;
	
	void start() throws ConnectorTaskException, InterruptedException;
	
	void stop() throws ConnectorTaskException, InterruptedException;
	
	void halt() throws ConnectorTaskException, InterruptedException;
	
	DeployedState getCurrentState();
	
	void updateCurrentState(DeployedState currentState);
	
}
