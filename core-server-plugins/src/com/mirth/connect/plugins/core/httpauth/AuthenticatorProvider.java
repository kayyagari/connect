/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.plugins.core.httpauth;

import com.mirth.connect.donkey.model.channel.ConnectorPluginProperties;
import com.mirth.connect.donkey.server.channel.IConnector;

public abstract class AuthenticatorProvider {

    private IConnector connector;
    private ConnectorPluginProperties properties;

    public AuthenticatorProvider(IConnector connector, ConnectorPluginProperties properties) {
        this.connector = connector;
        this.properties = properties;
    }

    public abstract Authenticator getAuthenticator() throws Exception;

    public void shutdown() {}

    public IConnector getConnector() {
        return connector;
    }

    public ConnectorPluginProperties getProperties() {
        return properties;
    }
}