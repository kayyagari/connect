/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.connectors.file;

import java.io.IOException;

import com.mirth.connect.connectors.core.file.FileConfiguration;
import com.mirth.connect.connectors.core.file.FileConnector;
import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.donkey.server.channel.IConnector;

public class DefaultFileConfiguration implements FileConfiguration {

    @Override
    public void configureConnectorDeploy(IConnector connector, ConnectorProperties connectorProperties) throws Exception {
        FileConnector fileConnector = new FileConnector(connector.getChannelId(), connectorProperties, connector);

        if (connector instanceof FileReceiver) {
            ((FileReceiver) connector).setFileConnector(fileConnector);
        } else if (connector instanceof FileDispatcher) {
            ((FileDispatcher) connector).setFileConnector(fileConnector);
        }
    }

    @Override
    public void configureConnectorUndeploy(IConnector connector) {}

    @Override
    public void initialize(Object client) throws IOException {}

    @Override
    public void disconnect(Object client) {}
}