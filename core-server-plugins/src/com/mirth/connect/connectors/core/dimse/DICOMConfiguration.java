/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.connectors.core.dimse;

import java.util.Map;

import com.mirth.connect.donkey.server.channel.IConnector;

public interface DICOMConfiguration {

    public void configureConnectorDeploy(IConnector connector) throws Exception;

    public Object createNetworkConnection();

    public void configureDcmRcv(IMirthDcmRcv dcmrcv, IDICOMReceiver connector, DICOMConnectorProperties connectorProperties) throws Exception;

    public void configureDcmSnd(IMirthDcmSnd dcmsnd, IConnector connector, IDICOMDispatcherProperties connectorProperties) throws Exception;

    public Map<String, Object> getCStoreRequestInformation(Object as);
}