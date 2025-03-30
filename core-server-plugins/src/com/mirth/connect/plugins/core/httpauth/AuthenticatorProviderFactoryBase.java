package com.mirth.connect.plugins.core.httpauth;

import com.mirth.connect.donkey.model.channel.ConnectorPluginProperties;
import com.mirth.connect.donkey.server.channel.IConnector;

public abstract class AuthenticatorProviderFactoryBase {

	 public abstract AuthenticatorProvider getAuthenticatorProviderNonStatic(IConnector connector, ConnectorPluginProperties properties) throws Exception;
}
