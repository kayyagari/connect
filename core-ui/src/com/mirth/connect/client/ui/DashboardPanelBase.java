package com.mirth.connect.client.ui;

import java.util.Set;

import javax.swing.JPanel;

import com.mirth.connect.model.DashboardStatus;

public abstract class DashboardPanelBase extends JPanel {

    protected static final String STATUS_COLUMN_NAME = "Status";
    protected static final String NAME_COLUMN_NAME = "Name";
    protected static final String RECEIVED_COLUMN_NAME = "Received";
    protected static final String QUEUED_COLUMN_NAME = "Queued";
    protected static final String SENT_COLUMN_NAME = "Sent";
    protected static final String ERROR_COLUMN_NAME = "Errored";
    protected static final String FILTERED_COLUMN_NAME = "Filtered";
    protected static final String LAST_DEPLOYED_COLUMN_NAME = "Last Deployed";
    protected static final String DEPLOYED_REVISION_DELTA_COLUMN_NAME = "Rev \u0394";
    protected static final String[] defaultColumns = new String[] { STATUS_COLUMN_NAME,
            NAME_COLUMN_NAME, DEPLOYED_REVISION_DELTA_COLUMN_NAME, LAST_DEPLOYED_COLUMN_NAME,
            RECEIVED_COLUMN_NAME, FILTERED_COLUMN_NAME, QUEUED_COLUMN_NAME, SENT_COLUMN_NAME,
            ERROR_COLUMN_NAME };

    public static int getNumberOfDefaultColumns() {
        return defaultColumns.length;
    }

    public abstract Set<DashboardStatus> getSelectedChannelStatuses();

    public abstract Object getCurrentTabPlugin();

    public abstract void loadPanelPlugin(Object plugin);
}
