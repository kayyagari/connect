/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.connectors.http;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletRequest;

import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.ssl.SSLContexts;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

import com.mirth.connect.connectors.core.http.HttpConfiguration;
import com.mirth.connect.connectors.core.http.IHttpDispatcher;
import com.mirth.connect.connectors.core.http.IHttpReceiver;
import com.mirth.connect.donkey.model.channel.ConnectorPluginProperties;
import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.donkey.server.channel.IConnector;
import com.mirth.connect.server.controllers.ConfigurationController;
import com.mirth.connect.server.controllers.ControllerFactory;
import com.mirth.connect.util.MirthSSLUtil;

public class DefaultHttpConfiguration implements HttpConfiguration {

    private ConfigurationController configurationController = ControllerFactory.getFactory().createConfigurationController();

    @Override
    public void configureConnectorDeploy(IConnector connector) throws Exception {
        if (connector instanceof IHttpDispatcher) {
            configureSocketFactoryRegistry(null, ((IHttpDispatcher) connector).getSocketFactoryRegistry());
        }
    }

    @Override
    public void configureConnectorUndeploy(IConnector connector) {}

    @Override
    public void configureReceiver(IHttpReceiver connector) throws Exception {
        org.eclipse.jetty.server.HttpConfiguration httpConfig = new org.eclipse.jetty.server.HttpConfiguration();
        httpConfig.setSendServerVersion(false);
        httpConfig.setSendXPoweredBy(false);
        
        ServerConnector listener = new ServerConnector((Server) connector.getServer(), new HttpConnectionFactory(httpConfig));
        listener.setHost(connector.getHost());
        listener.setPort(connector.getPort());
        listener.setIdleTimeout(connector.getTimeout());
        ((Server) connector.getServer()).addConnector(listener);
    }

    @Override
    public void configureDispatcher(IHttpDispatcher connector, ConnectorProperties connectorProperties) throws Exception {}

    @Override
    public void configureSocketFactoryRegistry(ConnectorPluginProperties properties, RegistryBuilder<ConnectionSocketFactory> registry) throws Exception {
        String[] enabledProtocols = MirthSSLUtil.getEnabledHttpsProtocols(configurationController.getHttpsClientProtocols());
        String[] enabledCipherSuites = MirthSSLUtil.getEnabledHttpsCipherSuites(configurationController.getHttpsCipherSuites());
        SSLConnectionSocketFactory sslConnectionSocketFactory = new SSLConnectionSocketFactory(SSLContexts.createSystemDefault(), enabledProtocols, enabledCipherSuites, NoopHostnameVerifier.INSTANCE);
        registry.register("https", sslConnectionSocketFactory);
    }

    @Override
    public Map<String, Object> getRequestInformation(ServletRequest request) {
        return new HashMap<String, Object>();
    }
}