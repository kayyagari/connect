/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.server.util;

import com.mirth.connect.client.core.ControllerException;
import com.mirth.connect.util.ClassUtil;
import com.mirth.connect.model.ServerSettings;
import com.mirth.connect.server.controllers.ControllerFactory;

public class ServerSMTPConnectionFactory {
	
    public static Class<?> SERVER_SMTP_CONNECTION;
	
	
    public static IServerSMTPConnection createSMTPConnection() throws ControllerException {
        try {
            TemplateValueReplacer replacer = new TemplateValueReplacer();
            ServerSettings settings = ControllerFactory.getFactory().createConfigurationController().getServerSettings();
            IServerSMTPConnection connection = ClassUtil.createInstanceOrDefault(IServerSMTPConnection.class, SERVER_SMTP_CONNECTION, null);
            connection.setHost(replacer.replaceValues(settings.getSmtpHost()));
            connection.setPort(replacer.replaceValues(settings.getSmtpPort()));
            connection.setSocketTimeout(Integer.parseInt(replacer.replaceValues(settings.getSmtpTimeout())));
            connection.setUseAuthentication(settings.getSmtpAuth());
            connection.setUsername(replacer.replaceValues(settings.getSmtpUsername()));
            connection.setFrom(replacer.replaceValues(settings.getSmtpFrom()));
            connection.setPassword( replacer.replaceValues(settings.getSmtpPassword()));
            connection.setSecure(settings.getSmtpSecure());
            return connection;
        } catch (Exception e) {
            if (e instanceof ControllerException) {
                throw (ControllerException) e;
            }
            throw new ControllerException(e);
        }
    }
}
