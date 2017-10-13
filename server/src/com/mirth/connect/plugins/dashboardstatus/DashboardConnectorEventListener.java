/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.plugins.dashboardstatus;

import java.awt.Color;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import com.mirth.connect.donkey.model.event.ConnectionStatusEventType;
import com.mirth.connect.donkey.model.event.Event;
import com.mirth.connect.donkey.server.event.EventType;
import com.mirth.connect.server.event.EventListener;

public class DashboardConnectorEventListener extends EventListener {

    private ConnectionLogController logController;
    
    public DashboardConnectorEventListener() {
		logController = ConnectionLogController.getInstance();
	}

    @Override
    protected void onShutdown() {}

    @Override
    public Set<EventType> getEventTypes() {
        return logController.getEventTypes();
    }

    @Override
    protected void processEvent(Event event) {
        logController.processEvent(event);
    }

    public Map<String, Object[]> getConnectorStateMap() {
        return logController.getConnectorStateMap();
    }

    public synchronized LinkedList<ConnectionLogItem> getChannelLog(Object object, int fetchSize, Long lastLogId) {
        return logController.getChannelLog(object, fetchSize, lastLogId);
    }

    public Color getColor(ConnectionStatusEventType type) {
        return logController.getColor(type);
    }

}
