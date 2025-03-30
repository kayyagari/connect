/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.connectors.ws;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;

import com.mirth.connect.connectors.core.ws.DefinitionServiceMap;
import com.mirth.connect.connectors.core.ws.IWebServiceDispatcherProperties;
import com.mirth.connect.connectors.core.ws.DefinitionServiceMap.DefinitionPortMap;
import com.mirth.connect.connectors.core.ws.DefinitionServiceMap.PortInformation;
import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.donkey.model.channel.DestinationConnectorProperties;
import com.mirth.connect.donkey.model.channel.DestinationConnectorPropertiesInterface;
import com.mirth.connect.donkey.util.DonkeyElement;
import com.mirth.connect.donkey.util.DonkeyElement.DonkeyElementException;
import com.mirth.connect.donkey.util.purge.PurgeUtil;
import com.mirth.connect.donkey.util.xstream.SerializerException;
import com.mirth.connect.model.converters.ObjectXMLSerializer;

public class WebServiceDispatcherProperties extends ConnectorProperties implements DestinationConnectorPropertiesInterface, IWebServiceDispatcherProperties {

    private DestinationConnectorProperties destinationConnectorProperties;

    private String wsdlUrl;
    private String service;
    private String port;
    private String operation;
    private String locationURI;
    private String socketTimeout;
    private boolean useAuthentication;
    private String username;
    private String password;
    private String envelope;
    private boolean oneWay;
    private Map<String, List<String>> headers;
    private String headersVariable;
    private boolean isUseHeadersVariable;
    private boolean useMtom;
    private List<String> attachmentNames;
    private List<String> attachmentContents;
    private List<String> attachmentTypes;
    private String attachmentsVariable;
    private boolean isUseAttachmentsVariable; // true to use attachments, false to use attachmentsVariable
    private String soapAction;
    private DefinitionServiceMap wsdlDefinitionMap;

    public static final String WEBSERVICE_DEFAULT_DROPDOWN = "Press Get Operations";

    public WebServiceDispatcherProperties() {
        destinationConnectorProperties = new DestinationConnectorProperties(false);

        this.wsdlUrl = "";
        this.operation = WEBSERVICE_DEFAULT_DROPDOWN;
        this.wsdlDefinitionMap = new DefinitionServiceMap();
        this.service = "";
        this.port = "";
        this.locationURI = "";
        this.socketTimeout = "30000";
        this.useAuthentication = false;
        this.username = "";
        this.password = "";
        this.envelope = "";
        this.oneWay = false;
        this.headers = new LinkedHashMap<String, List<String>>();
        this.isUseHeadersVariable = false;
        this.headersVariable = "";
        this.useMtom = false;
        this.attachmentNames = new ArrayList<String>();
        this.attachmentContents = new ArrayList<String>();
        this.attachmentTypes = new ArrayList<String>();
        this.isUseAttachmentsVariable = false;
        this.attachmentsVariable = "";
        this.soapAction = "";
    }

    public WebServiceDispatcherProperties(IWebServiceDispatcherProperties props) {
        super((ConnectorProperties) props);
        destinationConnectorProperties = new DestinationConnectorProperties(((DestinationConnectorPropertiesInterface) props).getDestinationConnectorProperties());

        wsdlUrl = props.getWsdlUrl();
        operation = props.getOperation();
        wsdlDefinitionMap = props.getWsdlDefinitionMap();
        service = props.getService();
        port = props.getPort();
        locationURI = props.getLocationURI();
        socketTimeout = props.getSocketTimeout();
        useAuthentication = props.isUseAuthentication();
        username = props.getUsername();
        password = props.getPassword();
        envelope = props.getEnvelope();
        oneWay = props.isOneWay();

        Map<String, List<String>> headerCopy = new LinkedHashMap<String, List<String>>();
        for (Entry<String, List<String>> entry : props.getHeadersMap().entrySet()) {
            headerCopy.put(entry.getKey(), new ArrayList<String>(entry.getValue()));
        }
        headers = headerCopy;
        isUseHeadersVariable = props.isUseHeadersVariable();
        headersVariable = props.getHeadersVariable();

        useMtom = props.isUseMtom();
        attachmentNames = new ArrayList<String>(props.getAttachmentNames());
        attachmentContents = new ArrayList<String>(props.getAttachmentContents());
        attachmentTypes = new ArrayList<String>(props.getAttachmentTypes());
        isUseAttachmentsVariable = props.isUseAttachmentsVariable();
        attachmentsVariable = props.getAttachmentsVariable();
        soapAction = props.getSoapAction();
    }

    @Override
    public String getWsdlUrl() {
        return wsdlUrl;
    }

    @Override
    public void setWsdlUrl(String wsdlUrl) {
        this.wsdlUrl = wsdlUrl;
    }

    @Override
    public String getService() {
        return service;
    }

    @Override
    public void setService(String service) {
        this.service = service;
    }

    @Override
    public String getPort() {
        return port;
    }

    @Override
    public void setPort(String port) {
        this.port = port;
    }

    @Override
    public String getLocationURI() {
        return locationURI;
    }

    @Override
    public void setLocationURI(String locationURI) {
        this.locationURI = locationURI;
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
    public String getOperation() {
        return operation;
    }

    @Override
    public void setOperation(String operation) {
        this.operation = operation;
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

    @Override
    public String getEnvelope() {
        return envelope;
    }

    @Override
    public void setEnvelope(String envelope) {
        this.envelope = envelope;
    }

    @Override
    public boolean isOneWay() {
        return oneWay;
    }

    @Override
    public void setOneWay(boolean oneWay) {
        this.oneWay = oneWay;
    }

    @Override
    public String getHeadersVariable() {
        return headersVariable;
    }

    @Override
    public void setHeadersVariable(String headersVariable) {
        this.headersVariable = headersVariable;
    }

    @Override
    public boolean isUseHeadersVariable() {
        return isUseHeadersVariable;
    }

    @Override
    public void setUseHeadersVariable(boolean isUseHeadersVariable) {
        this.isUseHeadersVariable = isUseHeadersVariable;
    }

    @Override
    public Map<String, List<String>> getHeadersMap() {
        return headers;
    }

    @Override
    public void setHeadersMap(Map<String, List<String>> headers) {
        this.headers = headers;
    }

    @Override
    public boolean isUseMtom() {
        return useMtom;
    }

    @Override
    public void setUseMtom(boolean useMtom) {
        this.useMtom = useMtom;
    }

    @Override
    public String getAttachmentsVariable() {
        return attachmentsVariable;
    }

    @Override
    public void setAttachmentsVariable(String attachmentsVariable) {
        this.attachmentsVariable = attachmentsVariable;
    }

    @Override
    public List<String> getAttachmentNames() {
        return attachmentNames;
    }

    @Override
    public void setAttachmentNames(List<String> attachmentNames) {
        this.attachmentNames = attachmentNames;
    }

    @Override
    public List<String> getAttachmentContents() {
        return attachmentContents;
    }

    @Override
    public void setAttachmentContents(List<String> attachmentContents) {
        this.attachmentContents = attachmentContents;
    }

    @Override
    public List<String> getAttachmentTypes() {
        return attachmentTypes;
    }

    @Override
    public void setAttachmentTypes(List<String> attachmentTypes) {
        this.attachmentTypes = attachmentTypes;
    }

    @Override
    public boolean isUseAttachmentsVariable() {
        return isUseAttachmentsVariable;
    }

    @Override
    public void setUseAttachmentsVariable(boolean isUseAttachmentsVariable) {
        this.isUseAttachmentsVariable = isUseAttachmentsVariable;
    }

    @Override
    public String getSoapAction() {
        return soapAction;
    }

    @Override
    public void setSoapAction(String soapAction) {
        this.soapAction = soapAction;
    }

    @Override
    public DefinitionServiceMap getWsdlDefinitionMap() {
        return wsdlDefinitionMap;
    }

    @Override
    public void setWsdlDefinitionMap(DefinitionServiceMap wsdlDefinitionMap) {
        this.wsdlDefinitionMap = wsdlDefinitionMap;
    }

    @Override
    public String getProtocol() {
        return "WS";
    }

    @Override
    public String getName() {
        return "Web Service Sender";
    }

    @Override
    public String toFormattedString() {
        StringBuilder builder = new StringBuilder();
        String newLine = "\n";

        builder.append("WSDL URL: ");
        builder.append(wsdlUrl);
        builder.append(newLine);

        if (StringUtils.isNotBlank(username)) {
            builder.append("USERNAME: ");
            builder.append(username);
            builder.append(newLine);
        }

        if (StringUtils.isNotBlank(service)) {
            builder.append("SERVICE: ");
            builder.append(service);
            builder.append(newLine);
        }

        if (StringUtils.isNotBlank(port)) {
            builder.append("PORT / ENDPOINT: ");
            builder.append(port);
            builder.append(newLine);
        }

        if (StringUtils.isNotBlank(locationURI)) {
            builder.append("LOCATION URI: ");
            builder.append(locationURI);
            builder.append(newLine);
        }

        if (StringUtils.isNotBlank(soapAction)) {
            builder.append("SOAP ACTION: ");
            builder.append(soapAction);
            builder.append(newLine);
        }

        if (isUseHeadersVariable()) {
            builder.append(newLine);
            builder.append("[HEADERS]");
            builder.append(newLine);
            builder.append("Using variable '" + getHeadersVariable() + "'");
        } else if (MapUtils.isNotEmpty(headers)) {
            builder.append(newLine);
            builder.append("[HEADERS]");
            builder.append(newLine);
            for (Map.Entry<String, List<String>> header : headers.entrySet()) {
                for (String value : (ArrayList<String>) header.getValue()) {
                    builder.append(header.getKey().toString());
                    builder.append(": ");
                    builder.append(value.toString());
                    builder.append(newLine);
                }
            }
        }

        builder.append(newLine);
        builder.append("[ATTACHMENTS]");
        if (isUseAttachmentsVariable()) {
            builder.append("Using variable '" + getAttachmentsVariable() + "'");
        } else {
            for (int i = 0; i < attachmentNames.size(); i++) {
                builder.append(newLine);
                builder.append(attachmentNames.get(i));
                builder.append(" (");
                builder.append(attachmentTypes.get(i));
                builder.append(")");
            }
        }
        builder.append(newLine);

        builder.append(newLine);
        builder.append("[CONTENT]");
        builder.append(newLine);
        builder.append(envelope);
        return builder.toString();
    }

    @Override
    public DestinationConnectorProperties getDestinationConnectorProperties() {
        return destinationConnectorProperties;
    }

    @Override
    public ConnectorProperties clone() {
        return new WebServiceDispatcherProperties(this);
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

        element.removeChild("wsdlCacheId");
        element.addChildElementIfNotExists("locationURI", "");

        DonkeyElement operations = element.removeChild("wsdlOperations");
        String service = element.getChildElement("service").getTextContent();
        String port = element.getChildElement("port").getTextContent();

        if (StringUtils.isNotBlank(service) && StringUtils.isNotBlank(port)) {
            DefinitionServiceMap wsdlDefinitionMap = new DefinitionServiceMap();
            DefinitionPortMap portMap = new DefinitionPortMap();
            List<String> operationList = new ArrayList<String>();

            if (operations != null) {
                for (DonkeyElement operation : operations.getChildElements()) {
                    operationList.add(operation.getTextContent());
                }
            }

            portMap.getMap().put(port, new PortInformation(operationList));
            wsdlDefinitionMap.getMap().put(service, portMap);

            try {
                if (element.getChildElement("wsdlDefinitionMap") == null) {
                    DonkeyElement definitionMapElement = element.addChildElementFromXml(ObjectXMLSerializer.getInstance().serialize(wsdlDefinitionMap));
                    definitionMapElement.setNodeName("wsdlDefinitionMap");
                }
            } catch (DonkeyElementException e) {
                throw new SerializerException("Failed to migrate Web Service Sender operation list.", e);
            }
        }

        element.addChildElementIfNotExists("socketTimeout", "0");

        Map<String, String> headers = new LinkedHashMap<String, String>();
        try {
            if (element.getChildElement("headers") == null) {
                DonkeyElement headersElement = element.addChildElementFromXml(ObjectXMLSerializer.getInstance().serialize(headers));
                headersElement.setNodeName("headers");
            }
        } catch (DonkeyElementException e) {
            throw new SerializerException("Failed to migrate Web Service Sender headers.", e);
        }
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
        DonkeyElement wsdlDefinitionMapElement = element.getChildElement("wsdlDefinitionMap");
        if (wsdlDefinitionMapElement != null) {
            DonkeyElement mapElement = wsdlDefinitionMapElement.getChildElement("map");
            if (mapElement != null) {
                for (DonkeyElement entryElement : mapElement.getChildElements()) {
                    for (DonkeyElement entrySubElement : entryElement.getChildElements()) {
                        if (StringUtils.equals("com.mirth.connect.connectors.ws.DefinitionServiceMap_-DefinitionPortMap", entrySubElement.getLocalName())) {
                            entrySubElement.setNodeName("com.mirth.connect.connectors.core.ws.DefinitionServiceMap_-DefinitionPortMap");

                            DonkeyElement subMapElement = entrySubElement.getChildElement("map");
                            if (subMapElement != null) {
                                for (DonkeyElement subMapEntryElement : subMapElement.getChildElements()) {
                                    for (DonkeyElement subMapEntrySubElement : subMapEntryElement.getChildElements()) {
                                        if (StringUtils.equals("com.mirth.connect.connectors.ws.DefinitionServiceMap_-PortInformation", subMapEntrySubElement.getLocalName())) {
                                            subMapEntrySubElement.setNodeName("com.mirth.connect.connectors.core.ws.DefinitionServiceMap_-PortInformation");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public Map<String, Object> getPurgedProperties() {
        Map<String, Object> purgedProperties = super.getPurgedProperties();
        purgedProperties.put("destinationConnectorProperties", destinationConnectorProperties.getPurgedProperties());
        purgedProperties.put("useAuthentication", useAuthentication);
        purgedProperties.put("envelopeLines", PurgeUtil.countLines(envelope));
        purgedProperties.put("oneWay", oneWay);
        purgedProperties.put("headersCount", headers.size());
        purgedProperties.put("useMtom", useMtom);
        purgedProperties.put("attachmentNamesCount", attachmentNames.size());
        purgedProperties.put("attachmentContentCount", attachmentContents.size());
        purgedProperties.put("wsdlDefinitionMapCount", wsdlDefinitionMap != null ? wsdlDefinitionMap.getMap().size() : 0);
        purgedProperties.put("socketTimeout", PurgeUtil.getNumericValue(socketTimeout));
        return purgedProperties;
    }
}
