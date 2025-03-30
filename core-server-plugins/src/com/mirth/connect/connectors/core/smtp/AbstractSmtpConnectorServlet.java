package com.mirth.connect.connectors.core.smtp;

import java.util.Properties;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.SecurityContext;

import com.mirth.connect.client.core.api.MirthApiException;
import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.server.api.MirthServlet;
import com.mirth.connect.server.controllers.ConfigurationController;
import com.mirth.connect.server.controllers.ControllerFactory;
import com.mirth.connect.server.util.TemplateValueReplacer;
import com.mirth.connect.util.ConnectionTestResponse;

public abstract class AbstractSmtpConnectorServlet extends MirthServlet implements SmtpConnectorServletInterface {
	
	protected static final TemplateValueReplacer replacer = new TemplateValueReplacer();
	protected static final ConfigurationController configurationController = ControllerFactory.getFactory().createConfigurationController();
	
    public AbstractSmtpConnectorServlet(HttpServletRequest request, SecurityContext sc) {
        super(request, sc);
    }	
	
    public AbstractSmtpConnectorServlet(HttpServletRequest request, SecurityContext sc, String extensionName) {
        super(request, sc, extensionName);
    }

    @Override
    public ConnectionTestResponse sendTestEmail(String channelId, String channelName, ConnectorProperties properties) {
        try {
            Properties props = new Properties();
            props.put("port", replacer.replaceValues(((ISmtpDispatcherProperties) properties).getSmtpPort(), channelId, channelName));
            props.put("encryption", ((ISmtpDispatcherProperties) properties).getEncryption());
            props.put("host", replacer.replaceValues(((ISmtpDispatcherProperties) properties).getSmtpHost(), channelId, channelName));
            props.put("timeout", ((ISmtpDispatcherProperties) properties).getTimeout());
            props.put("authentication", String.valueOf(((ISmtpDispatcherProperties) properties).isAuthentication()));
            props.put("username", replacer.replaceValues(((ISmtpDispatcherProperties) properties).getUsername(), channelId, channelName));
            props.put("password", replacer.replaceValues(((ISmtpDispatcherProperties) properties).getPassword(), channelId, channelName));
            props.put("toAddress", replacer.replaceValues(((ISmtpDispatcherProperties) properties).getTo(), channelId, channelName));
            props.put("fromAddress", replacer.replaceValues(((ISmtpDispatcherProperties) properties).getFrom(), channelId, channelName));

            return configurationController.sendTestEmail(props);
        } catch (Exception e) {
            throw new MirthApiException(e);
        }
    }

}
