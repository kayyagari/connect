/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.connectors.tcp;

import java.util.Map;

import org.apache.commons.lang3.builder.EqualsBuilder;

import com.mirth.connect.connectors.core.tcp.ITcpDispatcherProperties;
import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.donkey.model.channel.DestinationConnectorProperties;
import com.mirth.connect.donkey.model.channel.DestinationConnectorPropertiesInterface;
import com.mirth.connect.donkey.util.DonkeyElement;
import com.mirth.connect.donkey.util.purge.PurgeUtil;
import com.mirth.connect.model.transmission.TransmissionModeProperties;
import com.mirth.connect.model.transmission.framemode.FrameModeProperties;
import com.mirth.connect.util.CharsetUtils;
import com.mirth.connect.util.TcpUtil;

@SuppressWarnings("serial")
public class TcpDispatcherProperties extends ConnectorProperties implements DestinationConnectorPropertiesInterface, ITcpDispatcherProperties {

    private DestinationConnectorProperties destinationConnectorProperties;

    public static final String PROTOCOL = "TCP";
    public static final String NAME = "TCP Sender";

    private TransmissionModeProperties transmissionModeProperties;
    private boolean serverMode;
    private String remoteAddress;
    private String remotePort;
    private boolean overrideLocalBinding;
    private String localAddress;
    private String localPort;
    private String sendTimeout;
    private String bufferSize;
    private String maxConnections;
    private boolean keepConnectionOpen;
    private boolean checkRemoteHost;
    private String responseTimeout;
    private boolean ignoreResponse;
    private boolean queueOnResponseTimeout;
    private boolean dataTypeBinary;
    private String charsetEncoding;
    private String template;

    public TcpDispatcherProperties() {
        destinationConnectorProperties = new DestinationConnectorProperties(true);

        FrameModeProperties frameModeProperties = new FrameModeProperties("MLLP");
        frameModeProperties.setStartOfMessageBytes(TcpUtil.DEFAULT_LLP_START_BYTES);
        frameModeProperties.setEndOfMessageBytes(TcpUtil.DEFAULT_LLP_END_BYTES);
        this.transmissionModeProperties = frameModeProperties;

        this.serverMode = false; 
        this.remoteAddress = "127.0.0.1";
        this.remotePort = "6660";
        this.overrideLocalBinding = false;
        this.localAddress = "0.0.0.0";
        this.localPort = "0";
        this.sendTimeout = "5000";
        this.bufferSize = "65536";
        this.maxConnections = "10";
        this.keepConnectionOpen = false;
        this.checkRemoteHost = false;
        this.responseTimeout = "5000";
        this.ignoreResponse = false;
        this.queueOnResponseTimeout = true;
        this.dataTypeBinary = false;
        this.charsetEncoding = CharsetUtils.DEFAULT_ENCODING;
        this.template = "${message.encodedData}";
    }

    public TcpDispatcherProperties(ITcpDispatcherProperties props) {
        super((ConnectorProperties) props);
        destinationConnectorProperties = new DestinationConnectorProperties(((DestinationConnectorPropertiesInterface) props).getDestinationConnectorProperties());

        transmissionModeProperties = props.getTransmissionModeProperties();

        serverMode = props.isServerMode();
        remoteAddress = props.getRemoteAddress();
        remotePort = props.getRemotePort();
        overrideLocalBinding = props.isOverrideLocalBinding();
        localAddress = props.getLocalAddress();
        localPort = props.getLocalPort();
        sendTimeout = props.getSendTimeout();
        bufferSize = props.getBufferSize();
        keepConnectionOpen = props.isKeepConnectionOpen();
        checkRemoteHost = props.isCheckRemoteHost();
        responseTimeout = props.getResponseTimeout();
        ignoreResponse = props.isIgnoreResponse();
        queueOnResponseTimeout = props.isQueueOnResponseTimeout();
        dataTypeBinary = props.isDataTypeBinary();
        charsetEncoding = props.getCharsetEncoding();
        template = props.getTemplate();
    }

    @Override
    public TransmissionModeProperties getTransmissionModeProperties() {
        return transmissionModeProperties;
    }

    @Override
    public void setTransmissionModeProperties(TransmissionModeProperties transmissionModeProperties) {
        this.transmissionModeProperties = transmissionModeProperties;
    }
    
    @Override
    public boolean isServerMode() {
        return serverMode;
    }

    @Override
    public void setServerMode(boolean serverMode) {
        this.serverMode = serverMode;
    }

    @Override
    public String getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public void setRemoteAddress(String remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    @Override
    public String getRemotePort() {
        return remotePort;
    }

    @Override
    public void setRemotePort(String remotePort) {
        this.remotePort = remotePort;
    }

    @Override
    public boolean isOverrideLocalBinding() {
        return overrideLocalBinding;
    }

    @Override
    public void setOverrideLocalBinding(boolean overrideLocalBinding) {
        this.overrideLocalBinding = overrideLocalBinding;
    }

    @Override
    public String getLocalAddress() {
        return localAddress;
    }

    @Override
    public void setLocalAddress(String localAddress) {
        this.localAddress = localAddress;
    }

    @Override
    public String getLocalPort() {
        return localPort;
    }

    @Override
    public void setLocalPort(String localPort) {
        this.localPort = localPort;
    }

    @Override
    public String getSendTimeout() {
        return sendTimeout;
    }

    @Override
    public void setSendTimeout(String sendTimeout) {
        this.sendTimeout = sendTimeout;
    }

    @Override
    public String getBufferSize() {
        return bufferSize;
    }

    @Override
    public void setBufferSize(String bufferSize) {
        this.bufferSize = bufferSize;
    }
    
    @Override
    public String getMaxConnections() {
        return maxConnections;
    }

    @Override
    public void setMaxConnections(String maxConnections) {
        this.maxConnections = maxConnections;
    }

    @Override
    public boolean isKeepConnectionOpen() {
        return keepConnectionOpen;
    }

    @Override
    public void setKeepConnectionOpen(boolean keepConnectionOpen) {
        this.keepConnectionOpen = keepConnectionOpen;
    }

    @Override
    public boolean isCheckRemoteHost() {
        return checkRemoteHost;
    }

    @Override
    public void setCheckRemoteHost(boolean checkRemoteHost) {
        this.checkRemoteHost = checkRemoteHost;
    }

    @Override
    public String getResponseTimeout() {
        return responseTimeout;
    }

    @Override
    public void setResponseTimeout(String responseTimeout) {
        this.responseTimeout = responseTimeout;
    }

    @Override
    public boolean isIgnoreResponse() {
        return ignoreResponse;
    }

    @Override
    public void setIgnoreResponse(boolean ignoreResponse) {
        this.ignoreResponse = ignoreResponse;
    }

    @Override
    public boolean isQueueOnResponseTimeout() {
        return queueOnResponseTimeout;
    }

    @Override
    public void setQueueOnResponseTimeout(boolean queueOnResponseTimeout) {
        this.queueOnResponseTimeout = queueOnResponseTimeout;
    }

    @Override
    public boolean isDataTypeBinary() {
        return dataTypeBinary;
    }

    @Override
    public void setDataTypeBinary(boolean dataTypeBinary) {
        this.dataTypeBinary = dataTypeBinary;
    }

    @Override
    public String getCharsetEncoding() {
        return charsetEncoding;
    }

    @Override
    public void setCharsetEncoding(String charsetEncoding) {
        this.charsetEncoding = charsetEncoding;
    }

    @Override
    public String getTemplate() {
        return template;
    }

    @Override
    public void setTemplate(String template) {
        this.template = template;
    }

    @Override
    public String getProtocol() {
        return PROTOCOL;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String toFormattedString() {
        StringBuilder builder = new StringBuilder();
        String newLine = "\n";

        builder.append("REMOTE ADDRESS: ");
        builder.append(remoteAddress + ":" + remotePort);
        builder.append(newLine);

        if (overrideLocalBinding) {
            builder.append("LOCAL ADDRESS: ");
            builder.append(localAddress + ":" + localPort);
            builder.append(newLine);
        }

        builder.append(newLine);
        builder.append("[CONTENT]");
        builder.append(newLine);
        builder.append(template);
        return builder.toString();
    }

    @Override
    public DestinationConnectorProperties getDestinationConnectorProperties() {
        return destinationConnectorProperties;
    }

    @Override
    public void setDestinationConnectorProperties(DestinationConnectorProperties destinationConnectorProperties) {
        this.destinationConnectorProperties = destinationConnectorProperties;
    }

    @Override
    public ConnectorProperties clone() {
        return new TcpDispatcherProperties(this);
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

        element.addChildElementIfNotExists("checkRemoteHost", "true");

        DonkeyElement processHL7ACKElement = element.removeChild("processHL7ACK");
        DonkeyElement destinationPropertiesElement = element.getChildElement("destinationConnectorProperties");
        if (processHL7ACKElement != null && destinationPropertiesElement != null) {
            destinationPropertiesElement.addChildElement("validateResponse", processHL7ACKElement.getTextContent());
        }
    }

    // @formatter:off
    @Override public void migrate3_2_0(DonkeyElement element) {}
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
        purgedProperties.put("transmissionModeProperties", transmissionModeProperties.getPurgedProperties());
        purgedProperties.put("serverMode", serverMode);
        purgedProperties.put("overrideLocalBinding", overrideLocalBinding);
        purgedProperties.put("sendTimeout", PurgeUtil.getNumericValue(sendTimeout));
        purgedProperties.put("bufferSize", PurgeUtil.getNumericValue(bufferSize));
        purgedProperties.put("maxConnections", PurgeUtil.getNumericValue(maxConnections));
        purgedProperties.put("keepConnectionOpen", keepConnectionOpen);
        purgedProperties.put("checkRemoteHost", checkRemoteHost);
        purgedProperties.put("responseTimeout", PurgeUtil.getNumericValue(responseTimeout));
        purgedProperties.put("ignoreResponse", ignoreResponse);
        purgedProperties.put("queueOnResponseTimeout", queueOnResponseTimeout);
        purgedProperties.put("charsetEncoding", charsetEncoding);
        purgedProperties.put("templateLines", PurgeUtil.countLines(template));
        return purgedProperties;
    }
}
