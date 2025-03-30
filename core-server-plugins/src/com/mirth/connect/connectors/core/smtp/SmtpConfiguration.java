/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.connectors.core.smtp;

import java.util.Properties;

import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.donkey.server.channel.IConnector;

public interface SmtpConfiguration {

    public void configureConnectorDeploy(IConnector connector) throws Exception;

    public void configureEncryption(ConnectorProperties connectorProperties, Object email) throws Exception;

    public void configureMailProperties(Properties mailProperties) throws Exception;
}