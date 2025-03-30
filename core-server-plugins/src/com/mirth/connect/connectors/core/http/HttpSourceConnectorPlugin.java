package com.mirth.connect.connectors.core.http;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import com.mirth.connect.donkey.model.message.attachment.Attachment;
import com.mirth.connect.donkey.server.channel.DispatchResult;
import com.mirth.connect.donkey.server.channel.SourceConnectorPlugin;

public interface HttpSourceConnectorPlugin extends SourceConnectorPlugin {

	public void sendErrorResponse(Object baseRequest, HttpServletResponse servletResponse, DispatchResult dispatchResult, Throwable t) throws IOException;
	
	public Object getMessage(Object request, String contentType, Map<String, Object> sourceMap, List<Attachment> attachments) throws Exception;
	
}
