package com.mirth.connect.connectors.core.http;

import java.util.List;
import java.util.Map;

public interface IHttpReceiverProperties {
	
	public boolean isXmlBody();

    public void setXmlBody(boolean xmlBody);

    public boolean isParseMultipart();

    public void setParseMultipart(boolean parseMultipart);

    public boolean isIncludeMetadata();

    public void setIncludeMetadata(boolean includeMetadata);

    public String getBinaryMimeTypes();

    public void setBinaryMimeTypes(String binaryMimeTypes);

    public boolean isBinaryMimeTypesRegex();

    public void setBinaryMimeTypesRegex(boolean binaryMimeTypesRegex);

    public String getResponseContentType();

    public void setResponseContentType(String responseContentType);

    public boolean isResponseDataTypeBinary();

    public void setResponseDataTypeBinary(boolean responseDataTypeBinary);

    public String getResponseStatusCode();

    public void setResponseStatusCode(String responseStatusCode);
    
    public Map<String, List<String>> getResponseHeadersMap();

    public void setResponseHeadersMap(Map<String, List<String>> responseHeaders);

    public boolean isUseHeadersVariable();
    
    public void setUseHeadersVariable(boolean useResponseHeadersVariable);

    public void setResponseHeadersVariable(String headersVariable);
    
    public String getResponseHeadersVariable();
    
    public String getCharset();

    public void setCharset(String charset);

    public String getContextPath();

    public void setContextPath(String contextPath);

    public String getTimeout();

    public void setTimeout(String timeout);

    public List<HttpStaticResource> getStaticResources();

    public void setStaticResources(List<HttpStaticResource> staticResources);

}
