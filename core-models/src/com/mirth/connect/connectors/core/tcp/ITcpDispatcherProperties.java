package com.mirth.connect.connectors.core.tcp;

import com.mirth.connect.donkey.model.channel.DestinationConnectorProperties;
import com.mirth.connect.model.transmission.TransmissionModeProperties;

public interface ITcpDispatcherProperties extends TcpConnectorProperties {
	
	public TransmissionModeProperties getTransmissionModeProperties();
	
	public void setTransmissionModeProperties(TransmissionModeProperties transmissionModeProperties);

    public String getLocalAddress();
    
    public void setLocalAddress(String localAddress);

    public String getLocalPort();
    
    public void setLocalPort(String localPort);
    
	public String getResponseTimeout();
	
    public String getSendTimeout();

    public void setSendTimeout(String sendTimeout);

    public String getBufferSize();

    public void setBufferSize(String bufferSize);
    
    public String getMaxConnections();

    public void setMaxConnections(String maxConnections);

    public boolean isKeepConnectionOpen();

    public void setKeepConnectionOpen(boolean keepConnectionOpen);

    public boolean isCheckRemoteHost();

    public void setCheckRemoteHost(boolean checkRemoteHost);
    
    public void setResponseTimeout(String responseTimeout);

    public boolean isIgnoreResponse();

    public void setIgnoreResponse(boolean ignoreResponse);

    public boolean isQueueOnResponseTimeout();

    public void setQueueOnResponseTimeout(boolean queueOnResponseTimeout);

    public boolean isDataTypeBinary();

    public void setDataTypeBinary(boolean dataTypeBinary);

    public String getCharsetEncoding();

    public void setCharsetEncoding(String charsetEncoding);

    public String getTemplate();

    public void setTemplate(String template);
    
    public void setDestinationConnectorProperties(DestinationConnectorProperties destinationConnectorProperties);
	
}
