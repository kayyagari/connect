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
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;

import com.mirth.connect.connectors.core.http.IHttpDispatcherProperties;
import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.donkey.model.channel.DestinationConnectorProperties;
import com.mirth.connect.donkey.model.channel.DestinationConnectorPropertiesInterface;
import com.mirth.connect.donkey.model.channel.RemoteHostConnectorProperties;
import com.mirth.connect.donkey.util.DonkeyElement;
import com.mirth.connect.donkey.util.purge.PurgeUtil;

public class HttpDispatcherProperties extends ConnectorProperties implements DestinationConnectorPropertiesInterface, RemoteHostConnectorProperties, IHttpDispatcherProperties {

    private DestinationConnectorProperties destinationConnectorProperties;

    private String host;
    private boolean useProxyServer;
    private String proxyAddress;
    private String proxyPort;
    private String method;
    private Map<String, List<String>> headers;
    private Map<String, List<String>> parameters;
    private boolean useHeadersVariable;  // true to use headers, false to use headersTemplate
    private String headersVariable;
    private boolean useParametersVariable;  // true to use parameters, false to use parametersTemplate
    private String parametersVariable;
    private boolean responseXmlBody;
    private boolean responseParseMultipart;
    private boolean responseIncludeMetadata;
    private String responseBinaryMimeTypes;
    private boolean responseBinaryMimeTypesRegex;
    private boolean multipart;
    private boolean useAuthentication;
    private String authenticationType;
    private boolean usePreemptiveAuthentication;
    private String username;
    private String password;
    private String content;
    private String contentType;
    private boolean dataTypeBinary;
    private String charset;
    private String socketTimeout;

    public HttpDispatcherProperties() {
        destinationConnectorProperties = new DestinationConnectorProperties(false);

        this.host = "";
        this.useProxyServer = false;
        this.proxyAddress = "";
        this.proxyPort = "";
        this.method = "post";
        this.headers = new LinkedHashMap<String, List<String>>();
        this.useHeadersVariable = false;
        this.headersVariable = "";
        this.parameters = new LinkedHashMap<String, List<String>>();
        this.useParametersVariable = false;
        this.parametersVariable = "";
        this.responseXmlBody = false;
        this.responseParseMultipart = true;
        this.responseIncludeMetadata = false;
        this.responseBinaryMimeTypes = "application/.*(?<!json|xml)$|image/.*|video/.*|audio/.*";
        this.responseBinaryMimeTypesRegex = true;
        this.multipart = false;
        this.useAuthentication = false;
        this.authenticationType = "Basic";
        this.usePreemptiveAuthentication = false;
        this.username = "";
        this.password = "";
        this.content = "";
        this.contentType = "text/plain";
        this.dataTypeBinary = false;
        this.charset = "UTF-8";
        this.socketTimeout = "30000";
    }

    public HttpDispatcherProperties(HttpDispatcherProperties props) {
        super(props);
        destinationConnectorProperties = new DestinationConnectorProperties(props.getDestinationConnectorProperties());

        host = props.getHost();
        useProxyServer = props.isUseProxyServer();
        proxyAddress = props.getProxyAddress();
        proxyPort = props.getProxyPort();
        method = props.getMethod();

        headers = new LinkedHashMap<String, List<String>>();
        for (Entry<String, List<String>> entry : props.getHeadersMap().entrySet()) {
            headers.put(entry.getKey(), new ArrayList<String>(entry.getValue()));
        }
        useHeadersVariable = props.isUseHeadersVariable();
        headersVariable = props.getHeadersVariable();

        parameters = new LinkedHashMap<String, List<String>>();
        for (Entry<String, List<String>> entry : props.getParametersMap().entrySet()) {
            parameters.put(entry.getKey(), new ArrayList<String>(entry.getValue()));
        }
        useParametersVariable = props.isUseParametersVariable();
        parametersVariable = props.getParametersVariable();

        responseXmlBody = props.isResponseXmlBody();
        responseParseMultipart = props.isResponseParseMultipart();
        responseIncludeMetadata = props.isResponseIncludeMetadata();
        responseBinaryMimeTypes = props.getResponseBinaryMimeTypes();
        responseBinaryMimeTypesRegex = props.isResponseBinaryMimeTypesRegex();
        multipart = props.isMultipart();
        useAuthentication = props.isUseAuthentication();
        authenticationType = props.getAuthenticationType();
        usePreemptiveAuthentication = props.isUsePreemptiveAuthentication();
        username = props.getUsername();
        password = props.getPassword();
        content = props.getContent();
        contentType = props.getContentType();
        dataTypeBinary = props.isDataTypeBinary();
        charset = props.getCharset();
        socketTimeout = props.getSocketTimeout();
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public boolean isUseProxyServer() {
        return useProxyServer;
    }

    public void setUseProxyServer(boolean useProxyServer) {
        this.useProxyServer = useProxyServer;
    }

    @Override
    public String getProxyAddress() {
        return proxyAddress;
    }

    public void setProxyAddress(String proxyAddress) {
        this.proxyAddress = proxyAddress;
    }

    @Override
    public String getProxyPort() {
        return proxyPort;
    }

    public void setProxyPort(String proxyPort) {
        this.proxyPort = proxyPort;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    @Override
    public Map<String, List<String>> getHeadersMap() {
        return headers;
    }
    
    @Override
    public boolean isUseHeadersVariable() {
        return useHeadersVariable;
    }

    public void setUseHeadersVariable(boolean useHeadersVariable) {
        this.useHeadersVariable = useHeadersVariable;
    }

    public void setHeadersMap(Map<String, List<String>> headers) {
        this.headers = headers;
    }

    @Override
    public String getHeadersVariable() {
        return this.headersVariable;
    }
    
    public void setHeadersVariable(String headerVariable) {
        this.headersVariable = headerVariable;
    }

    public Map<String, List<String>> getParametersMap() {
        return parameters;
    }

    @Override
    public void setParametersMap(Map<String, List<String>> parameters) {
        this.parameters = parameters;
    }

    @Override
    public boolean isUseParametersVariable() {
        return useParametersVariable;
    }

    public void setUseParametersVariable(boolean useParametersVariable) {
        this.useParametersVariable = useParametersVariable;
    }

    @Override    
    public String getParametersVariable() {
        return this.parametersVariable;
    }
    
    public void setParametersVariable(String variableName) {
        this.parametersVariable = variableName;
    }

    @Override
    public boolean isResponseXmlBody() {
        return responseXmlBody;
    }

    public void setResponseXmlBody(boolean responseXmlBody) {
        this.responseXmlBody = responseXmlBody;
    }

    @Override
    public boolean isResponseParseMultipart() {
        return responseParseMultipart;
    }

    public void setResponseParseMultipart(boolean responseParseMultipart) {
        this.responseParseMultipart = responseParseMultipart;
    }

    @Override
    public boolean isResponseIncludeMetadata() {
        return responseIncludeMetadata;
    }

    public void setResponseIncludeMetadata(boolean responseIncludeMetadata) {
        this.responseIncludeMetadata = responseIncludeMetadata;
    }

    @Override
    public String getResponseBinaryMimeTypes() {
        return responseBinaryMimeTypes;
    }

    public void setResponseBinaryMimeTypes(String responseBinaryMimeTypes) {
        this.responseBinaryMimeTypes = responseBinaryMimeTypes;
    }

    @Override
    public boolean isResponseBinaryMimeTypesRegex() {
        return responseBinaryMimeTypesRegex;
    }

    public void setResponseBinaryMimeTypesRegex(boolean responseBinaryMimeTypesRegex) {
        this.responseBinaryMimeTypesRegex = responseBinaryMimeTypesRegex;
    }

    @Override
    public boolean isMultipart() {
        return multipart;
    }

    public void setMultipart(boolean multipart) {
        this.multipart = multipart;
    }

    @Override
    public boolean isUseAuthentication() {
        return useAuthentication;
    }

    @Override
    public void setUseAuthentication(boolean useAuthentication) {
        this.useAuthentication = useAuthentication;
    }

    @Override
    public String getAuthenticationType() {
        return authenticationType;
    }

    public void setAuthenticationType(String authenticationType) {
        this.authenticationType = authenticationType;
    }

    @Override
    public boolean isUsePreemptiveAuthentication() {
        return usePreemptiveAuthentication;
    }

    public void setUsePreemptiveAuthentication(boolean usePreemptiveAuthentication) {
        this.usePreemptiveAuthentication = usePreemptiveAuthentication;
    }

    public String getUsername() {
        return username;
    }

    @Override
    public void setUsername(String username) {
        this.username = username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public void setPassword(String password) {
        this.password = password;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String getContentType() {
        return contentType;
    }

    @Override
    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    @Override
    public boolean isDataTypeBinary() {
        return dataTypeBinary;
    }

    public void setDataTypeBinary(boolean dataTypeBinary) {
        this.dataTypeBinary = dataTypeBinary;
    }

    @Override
    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    @Override
    public String getSocketTimeout() {
        return socketTimeout;
    }

    @Override
    public void setSocketTimeout(String socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    @Override
    public String getProtocol() {
        return "HTTP";
    }

    @Override
    public String getName() {
        return "HTTP Sender";
    }

    @Override
    public String toFormattedString() {
        StringBuilder builder = new StringBuilder();
        String newLine = "\n";

        builder.append("URL: ");
        builder.append(host);
        builder.append(newLine);

        builder.append("METHOD: ");
        builder.append(method.toUpperCase());
        builder.append(newLine);

        if (StringUtils.isNotBlank(username)) {
            builder.append("USERNAME: ");
            builder.append(username);
            builder.append(newLine);
        }

        builder.append(newLine);
        builder.append("[HEADERS]");
        builder.append(newLine);
        if (isUseHeadersVariable()) {
            builder.append("Using variable '" + getHeadersVariable() + "'");
        } else {
        Map<String, List<String>> headers = getHeadersMap();
            for (Map.Entry<String, List<String>> header : headers.entrySet()) {
                for (String value : header.getValue()) {
                    builder.append(header.getKey().toString());
                    builder.append(": ");
                    builder.append(value.toString());
                    builder.append(newLine);
                }
            }
        }

        builder.append(newLine);
        builder.append("[PARAMETERS]");
        builder.append(newLine);
        if (isUseParametersVariable()) {
            builder.append("Using variable '" + getParametersVariable() + "'");
        } else {
            Map<String, List<String>> parameters = getParametersMap();
            for (Map.Entry<String, List<String>> parameter : parameters.entrySet()) {
                for (String value : parameter.getValue()) {
                    builder.append(parameter.getKey().toString());
                    builder.append(": ");
                    builder.append(value.toString());
                    builder.append(newLine);
                }
            }
        }

        builder.append(newLine);
        builder.append("[CONTENT]");
        builder.append(newLine);
        builder.append(content);
        return builder.toString();
    }

    @Override
    public DestinationConnectorProperties getDestinationConnectorProperties() {
        return destinationConnectorProperties;
    }

    @Override
    public ConnectorProperties clone() {
        return new HttpDispatcherProperties(this);
    }

    @Override
    public boolean canValidateResponse() {
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

        element.addChildElementIfNotExists("useProxyServer", "false");
        element.addChildElementIfNotExists("proxyAddress", "");
        element.addChildElementIfNotExists("proxyPort", "");

        boolean responseXmlBody = false;
        DonkeyElement includeHeadersElement = element.removeChild("includeHeadersInResponse");
        if (includeHeadersElement != null) {
            responseXmlBody = Boolean.parseBoolean(includeHeadersElement.getTextContent());
        }

        element.addChildElementIfNotExists("responseXmlBody", Boolean.toString(responseXmlBody));
        element.addChildElementIfNotExists("responseParseMultipart", Boolean.toString(!responseXmlBody));
        element.addChildElementIfNotExists("responseIncludeMetadata", Boolean.toString(responseXmlBody));

        if (responseXmlBody) {
            element.addChildElementIfNotExists("responseBinaryMimeTypes", "application/, image/, video/, audio/");
        } else {
            element.addChildElementIfNotExists("responseBinaryMimeTypes");
        }
        element.addChildElementIfNotExists("responseBinaryMimeTypesRegex", "false");

        boolean useAuthentication = false;
        DonkeyElement useAuthenticationElement = element.getChildElement("useAuthentication");
        if (useAuthenticationElement != null) {
            useAuthentication = Boolean.parseBoolean(useAuthenticationElement.getTextContent());
        }
        element.addChildElementIfNotExists("usePreemptiveAuthentication", Boolean.toString(useAuthentication));

        element.addChildElementIfNotExists("dataTypeBinary", "false");
    }

    @Override
    public void migrate3_2_0(DonkeyElement element) {
        if (element.getChildElement("headers") != null) {
            DonkeyElement oldHeaders = element.removeChild("headers");
            DonkeyElement newHeaders = element.addChildElement("headers");
            newHeaders.setAttribute("class", "linked-hash-map");

            for (DonkeyElement oldEntry : oldHeaders.getChildElements()) {
                if (oldEntry.getChildElements().size() >= 2) {
                    DonkeyElement entry = newHeaders.addChildElement("entry");
                    entry.addChildElement("string", oldEntry.getChildElements().get(0).getTextContent());
                    entry.addChildElement("list").addChildElement("string", oldEntry.getChildElements().get(1).getTextContent());
                }
            }
        }

        if (element.getChildElement("parameters") != null) {
            DonkeyElement oldParameters = element.removeChild("parameters");
            DonkeyElement newParameters = element.addChildElement("parameters");
            newParameters.setAttribute("class", "linked-hash-map");

            for (DonkeyElement oldEntry : oldParameters.getChildElements()) {
                if (oldEntry.getChildElements().size() >= 2) {
                    DonkeyElement entry = newParameters.addChildElement("entry");
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
    public Map<String, Object> getPurgedProperties() {
        Map<String, Object> purgedProperties = super.getPurgedProperties();
        purgedProperties.put("destinationConnectorProperties", destinationConnectorProperties.getPurgedProperties());
        purgedProperties.put("method", method);
        purgedProperties.put("headerCount", headers.size());
        purgedProperties.put("parameterCount", parameters.size());
        purgedProperties.put("multipart", multipart);
        purgedProperties.put("responseBinaryMimeTypesRegex", responseBinaryMimeTypesRegex);
        purgedProperties.put("useAuthentication", useAuthentication);
        purgedProperties.put("contentLines", PurgeUtil.countLines(content));
        purgedProperties.put("dataTypeBinary", dataTypeBinary);
        purgedProperties.put("charset", charset);
        purgedProperties.put("socketTimeout", PurgeUtil.getNumericValue(socketTimeout));
        return purgedProperties;
    }
}