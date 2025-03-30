package com.mirth.connect.connectors.core.http;

import java.util.List;
import java.util.Map;

import com.mirth.connect.donkey.model.channel.DestinationConnectorProperties;

public interface IHttpDispatcherProperties {
    
    String getMethod();
    
    void setMethod(String method);
    
    String getProtocol();
    
    String toFormattedString();
    
    String getHost();
    
    void setHost(String host);
    
    Map<String, List<String>> getParametersMap();
    
    String getUsername();
    
    String getContent();
    
    void setContent(String string);
    
    Map<String, Object> getPurgedProperties();
    
    String getName();
    
    void setSocketTimeout(String socketTimeout);
    
    Map<String, List<String>> getHeadersMap();

    void setUseAuthentication(boolean useAuthentication);
    
    void setUsername(String username);
    
    void setPassword(String password);
    
    void setParametersMap(Map<String, List<String>> queryPathParameters);

    String getSocketTimeout();
    
    boolean isUseAuthentication();

    String getPassword();

    boolean isUseParametersVariable();

    String getParametersVariable();    
    
    void setContentType(String contentType);
    
    boolean isResponseBinaryMimeTypesRegex();
    
    public boolean isUsePreemptiveAuthentication();
    
    public String getAuthenticationType() ;
    
    public String getCharset();
    
    public String getContentType();
    
    public DestinationConnectorProperties getDestinationConnectorProperties();

	boolean isUseProxyServer();

	String getProxyAddress();

	String getProxyPort();

	boolean isUseHeadersVariable();

	String getHeadersVariable();

	boolean isResponseXmlBody();

	boolean isResponseParseMultipart();

	boolean isResponseIncludeMetadata();

	String getResponseBinaryMimeTypes();

	boolean isMultipart();

	boolean isDataTypeBinary();
}
