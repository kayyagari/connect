/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.connectors.http;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.EqualsBuilder;

import com.mirth.connect.connectors.core.http.HttpStaticResource;
import com.mirth.connect.connectors.core.http.IHttpReceiverProperties;
import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.donkey.model.channel.ListenerConnectorProperties;
import com.mirth.connect.donkey.model.channel.ListenerConnectorPropertiesInterface;
import com.mirth.connect.donkey.model.channel.SourceConnectorProperties;
import com.mirth.connect.donkey.model.channel.SourceConnectorPropertiesInterface;
import com.mirth.connect.donkey.util.DonkeyElement;
import com.mirth.connect.donkey.util.purge.PurgeUtil;

public class HttpReceiverProperties extends ConnectorProperties implements ListenerConnectorPropertiesInterface, SourceConnectorPropertiesInterface, IHttpReceiverProperties {
    private ListenerConnectorProperties listenerConnectorProperties;
    private SourceConnectorProperties sourceConnectorProperties;

    private boolean xmlBody;
    private boolean parseMultipart;
    private boolean includeMetadata;
    private String binaryMimeTypes;
    private boolean binaryMimeTypesRegex;
    private String responseContentType;
    private boolean responseDataTypeBinary;
    private String responseStatusCode;
    private Map<String, List<String>> responseHeaders;
    private String responseHeadersVariable;
    private boolean useResponseHeadersVariable;  // true to use responseHeaders, false to use responseHeadersVariable
    private String charset;
    private String contextPath;
    private String timeout;
    private List<HttpStaticResource> staticResources;

    public HttpReceiverProperties() {
        listenerConnectorProperties = new ListenerConnectorProperties("80");
        sourceConnectorProperties = new SourceConnectorProperties();

        this.xmlBody = false;
        this.parseMultipart = true;
        this.includeMetadata = false;
        this.binaryMimeTypes = "application/.*(?<!json|xml)$|image/.*|video/.*|audio/.*";
        this.binaryMimeTypesRegex = true;
        this.responseContentType = "text/plain";
        this.responseDataTypeBinary = false;
        this.responseStatusCode = "";
        this.responseHeaders = new LinkedHashMap<String, List<String>>();
        this.charset = "UTF-8";
        this.contextPath = "";
        this.timeout = "30000";
        this.staticResources = new ArrayList<HttpStaticResource>();
        this.responseHeadersVariable = "";
        this.useResponseHeadersVariable = false;
    }
    
    public HttpReceiverProperties(IHttpReceiverProperties props) {
    	super((ConnectorProperties) props);
    	
    	listenerConnectorProperties = new ListenerConnectorProperties(((ListenerConnectorPropertiesInterface) props).getListenerConnectorProperties());
    	sourceConnectorProperties = new SourceConnectorProperties(((SourceConnectorPropertiesInterface) props).getSourceConnectorProperties());
    	
    	xmlBody = props.isXmlBody();
    	parseMultipart = props.isParseMultipart();
    	includeMetadata = props.isIncludeMetadata();
        binaryMimeTypes = props.getBinaryMimeTypes();
        binaryMimeTypesRegex = props.isBinaryMimeTypesRegex();
        responseContentType = props.getResponseContentType();
        responseDataTypeBinary = props.isResponseDataTypeBinary();
        responseStatusCode = props.getResponseStatusCode(); 
        responseHeaders = props.getResponseHeadersMap();
        charset = props.getCharset();
        contextPath = props.getContextPath(); 
        timeout = props.getTimeout();
        staticResources = props.getStaticResources();
        responseHeadersVariable = props.getResponseHeadersVariable(); 
        useResponseHeadersVariable = props.isUseHeadersVariable();
    }

    @Override
    public boolean isXmlBody() {
        return xmlBody;
    }

    @Override
    public void setXmlBody(boolean xmlBody) {
        this.xmlBody = xmlBody;
    }

    @Override
    public boolean isParseMultipart() {
        return parseMultipart;
    }

    @Override
    public void setParseMultipart(boolean parseMultipart) {
        this.parseMultipart = parseMultipart;
    }

    @Override
    public boolean isIncludeMetadata() {
        return includeMetadata;
    }

    @Override
    public void setIncludeMetadata(boolean includeMetadata) {
        this.includeMetadata = includeMetadata;
    }

    @Override
    public String getBinaryMimeTypes() {
        return binaryMimeTypes;
    }

    @Override
    public void setBinaryMimeTypes(String binaryMimeTypes) {
        this.binaryMimeTypes = binaryMimeTypes;
    }
    
    @Override
    public boolean isBinaryMimeTypesRegex() {
        return binaryMimeTypesRegex;
    }

    @Override
    public void setBinaryMimeTypesRegex(boolean binaryMimeTypesRegex) {
        this.binaryMimeTypesRegex = binaryMimeTypesRegex;
    }

    @Override
    public String getResponseContentType() {
        return responseContentType;
    }

    @Override
    public void setResponseContentType(String responseContentType) {
        this.responseContentType = responseContentType;
    }

    @Override
    public boolean isResponseDataTypeBinary() {
        return responseDataTypeBinary;
    }

    @Override
    public void setResponseDataTypeBinary(boolean responseDataTypeBinary) {
        this.responseDataTypeBinary = responseDataTypeBinary;
    }

    @Override
    public String getResponseStatusCode() {
        return responseStatusCode;
    }

    @Override
    public void setResponseStatusCode(String responseStatusCode) {
        this.responseStatusCode = responseStatusCode;
    }
    
    @Override
    public Map<String, List<String>> getResponseHeadersMap() {
        return responseHeaders;
    }

    @Override
    public void setResponseHeadersMap(Map<String, List<String>> responseHeaders) {
        this.responseHeaders = responseHeaders;
    }

    @Override
    public boolean isUseHeadersVariable() {
        return useResponseHeadersVariable;
    }
    
    @Override
    public void setUseHeadersVariable(boolean useResponseHeadersVariable) {
        this.useResponseHeadersVariable = useResponseHeadersVariable;
    }

    @Override
    public void setResponseHeadersVariable(String headersVariable) {
        this.responseHeadersVariable = headersVariable;
    }
    
    @Override
    public String getResponseHeadersVariable() {
        return this.responseHeadersVariable;
    }
    
    @Override
    public String getCharset() {
        return charset;
    }

    @Override
    public void setCharset(String charset) {
        this.charset = charset;
    }

    @Override
    public String getContextPath() {
        return contextPath;
    }

    @Override
    public void setContextPath(String contextPath) {
        this.contextPath = contextPath;
    }

    @Override
    public String getTimeout() {
        return timeout;
    }

    @Override
    public void setTimeout(String timeout) {
        this.timeout = timeout;
    }

    @Override
    public List<HttpStaticResource> getStaticResources() {
        return staticResources;
    }

    @Override
    public void setStaticResources(List<HttpStaticResource> staticResources) {
        this.staticResources = staticResources;
    }

    @Override
    public String getProtocol() {
        return "HTTP";
    }

    @Override
    public String getName() {
        return "HTTP Listener";
    }

    @Override
    public String toFormattedString() {
        return null;
    }

    @Override
    public ListenerConnectorProperties getListenerConnectorProperties() {
        return listenerConnectorProperties;
    }

    @Override
    public SourceConnectorProperties getSourceConnectorProperties() {
        return sourceConnectorProperties;
    }

    @Override
    public boolean canBatch() {
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    // @formatter:off
    @Override public void migrate3_0_1(DonkeyElement element) {}
    @Override public void migrate3_0_2(DonkeyElement element) {} // @formatter:on

    @Override
    public void migrate3_1_0(DonkeyElement element) {
        super.migrate3_1_0(element);

        boolean xmlBody = false;
        DonkeyElement bodyOnlyElement = element.removeChild("bodyOnly");
        if (bodyOnlyElement != null) {
            xmlBody = !Boolean.parseBoolean(bodyOnlyElement.getTextContent());
        }

        element.addChildElementIfNotExists("xmlBody", Boolean.toString(xmlBody));
        element.addChildElementIfNotExists("parseMultipart", Boolean.toString(!xmlBody));
        element.addChildElementIfNotExists("includeMetadata", Boolean.toString(xmlBody));

        if (xmlBody) {
            element.addChildElementIfNotExists("binaryMimeTypes", "application/, image/, video/, audio/");
        } else {
            element.addChildElementIfNotExists("binaryMimeTypes");
        }
        element.addChildElementIfNotExists("binaryMimeTypesRegex", "false");

        element.addChildElementIfNotExists("responseDataTypeBinary", "false");

        element.addChildElementIfNotExists("staticResources");
    }

    @Override
    public void migrate3_2_0(DonkeyElement element) {
        if (element.getChildElement("responseHeaders") != null) {
            DonkeyElement oldHeaders = element.removeChild("responseHeaders");
            DonkeyElement newHeaders = element.addChildElement("responseHeaders");
            newHeaders.setAttribute("class", "linked-hash-map");

            for (DonkeyElement oldEntry : oldHeaders.getChildElements()) {
                if (oldEntry.getChildElements().size() >= 2) {
                    DonkeyElement entry = newHeaders.addChildElement("entry");
                    entry.addChildElement("string", oldEntry.getChildElements().get(0).getTextContent());
                    entry.addChildElement("list").addChildElement("string", oldEntry.getChildElements().get(1).getTextContent());
                }
            }
        }
    }

    // @formatter:off
    @Override public void migrate3_3_0(DonkeyElement element) {}
    @Override public void migrate3_4_0(DonkeyElement element) {}
    @Override public void migrate3_5_0(DonkeyElement element) {}
    @Override public void migrate3_6_0(DonkeyElement element) {}
    @Override public void migrate3_7_0(DonkeyElement element) {}
    @Override public void migrate3_9_0(DonkeyElement element) {}
    @Override public void migrate3_11_0(DonkeyElement element) {}
    @Override public void migrate3_11_1(DonkeyElement element) {} 
    @Override public void migrate3_12_0(DonkeyElement element) {}// @formatter:on
    
    @Override
    public void migrate4_6_0(DonkeyElement element) {
    	super.migrate4_6_0(element);
    	
    	DonkeyElement staticResourcesEl = element.getChildElement("staticResources");
    	if (staticResourcesEl != null) {
    		List<DonkeyElement> resources = staticResourcesEl.getChildElements();
    		
    		for (DonkeyElement resource : resources) {
    			if (resource.getNodeName().equals("com.mirth.connect.connectors.http.HttpStaticResource")) {
    				resource.setNodeName("com.mirth.connect.connectors.core.http.HttpStaticResource");
    			}
    		}
    	}
    }

    @Override
    public Map<String, Object> getPurgedProperties() {
        Map<String, Object> purgedProperties = super.getPurgedProperties();
        purgedProperties.put("sourceConnectorProperties", sourceConnectorProperties.getPurgedProperties());
        purgedProperties.put("binaryMimeTypesRegex", binaryMimeTypesRegex);
        purgedProperties.put("responseDataTypeBinary", responseDataTypeBinary);
        purgedProperties.put("responseHeaderChars", responseHeaders.size());
        purgedProperties.put("charset", charset);
        purgedProperties.put("timeout", PurgeUtil.getNumericValue(timeout));
        return purgedProperties;
    }
}
