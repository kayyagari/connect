package com.mirth.connect.connectors.core.email;

import java.util.Properties;

import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.donkey.server.channel.ISourceConnector;

public interface EmailConfiguration {

    public void configureConnectorDeploy(ISourceConnector connector, ConnectorProperties connectorProperties) throws Exception;

    public void configureEncryption(ConnectorProperties connectorProperties, IEmailClient client) throws Exception;

    public void configureMailProperties(Properties mailProperties, String prefix, String encryption) throws Exception;
}