/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.connectors.dimse;

import java.util.HashMap;
import java.util.Map;

import org.dcm4che2.net.NetworkConnection;

import com.mirth.connect.connectors.core.dimse.DICOMConfiguration;
import com.mirth.connect.connectors.core.dimse.DICOMConfigurationUtil;
import com.mirth.connect.connectors.core.dimse.DICOMConnectorProperties;
import com.mirth.connect.connectors.core.dimse.IDICOMDispatcherProperties;
import com.mirth.connect.connectors.core.dimse.IDICOMReceiver;
import com.mirth.connect.connectors.core.dimse.IMirthDcmRcv;
import com.mirth.connect.connectors.core.dimse.IMirthDcmSnd;
import com.mirth.connect.donkey.server.channel.IConnector;
import com.mirth.connect.server.controllers.ConfigurationController;
import com.mirth.connect.server.controllers.ControllerFactory;
import com.mirth.connect.util.MirthSSLUtil;

public class DefaultDICOMConfiguration implements DICOMConfiguration {

    private ConfigurationController configurationController = ControllerFactory.getFactory().createConfigurationController();
    private String[] protocols;

    @Override
    public void configureConnectorDeploy(IConnector connector) throws Exception {
        if (connector instanceof DICOMReceiver) {
            protocols = MirthSSLUtil.getEnabledHttpsProtocols(configurationController.getHttpsServerProtocols());
        } else {
            protocols = MirthSSLUtil.getEnabledHttpsProtocols(configurationController.getHttpsClientProtocols());
        }
    }

    @Override
    public Object createNetworkConnection() {
        return new NetworkConnection();
    }

    @Override
    public void configureDcmRcv(IMirthDcmRcv dcmrcv, IDICOMReceiver connector, DICOMConnectorProperties connectorProperties) throws Exception {
        DICOMConfigurationUtil.configureDcmRcv(dcmrcv, connector, connectorProperties, protocols);
    }

    @Override
    public void configureDcmSnd(IMirthDcmSnd dcmsnd, IConnector connector, IDICOMDispatcherProperties connectorProperties) throws Exception {
        DICOMConfigurationUtil.configureDcmSnd(dcmsnd, connector, connectorProperties, protocols);
    }

    @Override
    public Map<String, Object> getCStoreRequestInformation(Object as) {
        return new HashMap<String, Object>();
    }
}