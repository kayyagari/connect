/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.connectors.file;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;

import com.mirth.connect.connectors.core.file.AbstractFileConnectorServlet;
import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.util.ConnectionTestResponse;

public class FileConnectorServlet extends AbstractFileConnectorServlet {

    public FileConnectorServlet(@Context HttpServletRequest request, @Context SecurityContext sc) {
        super(request, sc, PLUGIN_POINT);
    }

    @Override
    public ConnectionTestResponse testRead(String channelId, String channelName, ConnectorProperties properties) {
        return testReadOrWrite(channelId, channelName, properties, ((FileReceiverProperties) properties).getHost(), ((FileReceiverProperties) properties).isAnonymous(), ((FileReceiverProperties) properties).getUsername(), ((FileReceiverProperties) properties).getPassword(), ((FileReceiverProperties) properties).getSchemeProperties(), ((FileReceiverProperties) properties).getScheme(), ((FileReceiverProperties) properties).getTimeout(), ((FileReceiverProperties) properties).isSecure(), ((FileReceiverProperties) properties).isPassive(), true);
    }

    @Override
    public ConnectionTestResponse testWrite(String channelId, String channelName, ConnectorProperties properties) {
        return testReadOrWrite(channelId, channelName, properties, ((FileReceiverProperties) properties).getHost(), ((FileReceiverProperties) properties).isAnonymous(), ((FileReceiverProperties) properties).getUsername(), ((FileReceiverProperties) properties).getPassword(), ((FileReceiverProperties) properties).getSchemeProperties(), ((FileReceiverProperties) properties).getScheme(), ((FileReceiverProperties) properties).getTimeout(), ((FileReceiverProperties) properties).isSecure(), ((FileReceiverProperties) properties).isPassive(), false);
    }

}
