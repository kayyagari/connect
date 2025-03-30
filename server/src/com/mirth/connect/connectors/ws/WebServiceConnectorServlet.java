/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.connectors.ws;

import java.net.URL;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;
import javax.wsdl.Definition;
import javax.wsdl.factory.WSDLFactory;
import javax.wsdl.xml.WSDLReader;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import com.mirth.connect.client.core.api.MirthApiException;
import com.mirth.connect.connectors.core.ws.AbstractWebServiceConnectorServlet;
import com.mirth.connect.connectors.core.ws.IWebServiceDispatcherProperties;
import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.server.util.ConnectorUtil;
import com.mirth.connect.util.ConnectionTestResponse;

public class WebServiceConnectorServlet extends AbstractWebServiceConnectorServlet {

    public WebServiceConnectorServlet(@Context HttpServletRequest request, @Context SecurityContext sc) {
        super(request, sc, PLUGIN_POINT);
    }

    @Override
    public Object cacheWsdlFromUrl(String channelId, String channelName, ConnectorProperties properties) {
        try {
            String wsdlUrl = getWsdlUrl(channelId, channelName, ((IWebServiceDispatcherProperties) properties).getWsdlUrl(), ((IWebServiceDispatcherProperties) properties).getUsername(), ((IWebServiceDispatcherProperties) properties).getPassword());
            cacheWsdlInterfaces(wsdlUrl, getDefinition(wsdlUrl, properties, channelId));
            return null;
        } catch (Exception e) {
            throw new MirthApiException(e);
        }
    }

    @Override
    public ConnectionTestResponse testConnection(String channelId, String channelName, ConnectorProperties properties) {
        try {
            // Test the Location URI first if populated. Otherwise test the WSDL URL
            if (StringUtils.isNotBlank(((IWebServiceDispatcherProperties) properties).getLocationURI())) {
                return testConnection(channelId, channelName, ((IWebServiceDispatcherProperties) properties).getLocationURI());
            } else if (StringUtils.isNotBlank(((IWebServiceDispatcherProperties) properties).getWsdlUrl())) {
                return testConnection(channelId, channelName, ((IWebServiceDispatcherProperties) properties).getWsdlUrl());
            } else {
                throw new Exception("Both WSDL URL and Location URI are blank. At least one must be populated in order to test connection.");
            }
        } catch (Exception e) {
            throw new MirthApiException(e);
        }
    }

    protected ConnectionTestResponse testConnection(String channelId, String channelName, String urlString) throws Exception {
        URL url = new URL(replacer.replaceValues(urlString, channelId, channelName));
        int port = url.getPort();
        // If no port was provided, default to port 80 or 443.
        return ConnectorUtil.testConnection(url.getHost(), (port == -1) ? (StringUtils.equalsIgnoreCase(url.getProtocol(), "https") ? 443 : 80) : port, MAX_TIMEOUT);
    }

    /*
     * Retrieves the WSDL interface from the specified URL (with optional credentials). Uses a
     * Future to execute the request in the background and timeout after 30 seconds if the server
     * could not be contacted.
     */
    private Definition getDefinition(String wsdlUrl, ConnectorProperties props, String channelId) throws Exception {
        WSDLFactory wsdlFactory = WSDLFactory.newInstance();
        WSDLReader wsdlReader = wsdlFactory.newWSDLReader();
        int timeout = NumberUtils.toInt(((IWebServiceDispatcherProperties) props).getSocketTimeout());
        return importWsdlInterfaces(wsdlFactory, wsdlUrl, wsdlReader, timeout);
    }

}