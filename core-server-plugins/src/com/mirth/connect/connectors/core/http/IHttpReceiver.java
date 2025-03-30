package com.mirth.connect.connectors.core.http;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.apache.http.entity.ContentType;

import com.mirth.connect.donkey.server.ConnectorTaskException;
import com.mirth.connect.donkey.server.channel.DispatchResult;
import com.mirth.connect.donkey.server.channel.ISourceConnector;

public interface IHttpReceiver extends ISourceConnector {

    public Object getServer();

    public String getHost();

    public int getPort();

    public int getTimeout();
    
    public String replaceValues(String template, DispatchResult dispatchResult);
    
	public void doHandleRecoveredResponse(DispatchResult dispatchResult);

    public void doOnDeploy() throws ConnectorTaskException;
    
    public void doOnUndeploy() throws ConnectorTaskException;

    public void doOnStart() throws ConnectorTaskException;

    public void doOnStop() throws ConnectorTaskException;

    public void doOnHalt() throws ConnectorTaskException;
    
    public void doSendResponse(Object baseRequest, HttpServletResponse servletResponse, DispatchResult dispatchResult) throws Exception;
    
    public void sendResponse(Object baseRequest, HttpServletResponse servletResponse, DispatchResult dispatchResult, ContentType contentType, Map<String, List<String>> responseHeaders, byte[] responseBytes) throws Exception;
    
    public void doSendErrorResponse(Object baseRequest, HttpServletResponse servletResponse, DispatchResult dispatchResult, Throwable t) throws IOException;
    
    public HttpRequestMessage createRequestMessage(Object request, boolean ignorePayload) throws Exception;
    
    public HttpRequestMessage createRequestMessage(Object requestObj, boolean ignorePayload, boolean parseMultipart) throws Exception;
    
    public void populateSourceMap(Object request, HttpRequestMessage requestMessage, Map<String, Object> sourceMap);
    
    public void doPopulateSourceMap(Object request, HttpRequestMessage requestMessage, Map<String, Object> sourceMap);
    
}
