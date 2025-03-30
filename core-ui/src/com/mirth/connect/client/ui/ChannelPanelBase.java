package com.mirth.connect.client.ui;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.JPopupMenu;

import org.jdesktop.swingx.JXTaskPane;

import com.mirth.connect.model.Channel;
import com.mirth.connect.model.ChannelDependency;
import com.mirth.connect.model.ChannelHeader;
import com.mirth.connect.model.ChannelStatus;
import com.mirth.connect.model.ChannelSummary;
import com.mirth.connect.model.ChannelTag;
import com.mirth.connect.model.DashboardStatus;

public abstract class ChannelPanelBase extends AbstractFramePanel {

    public abstract Map<String, String> getCachedChannelIdsAndNames();

    public abstract Map<String, ChannelStatus> getCachedChannelStatuses();

    public abstract Map<String, ChannelGroupStatus> getCachedGroupStatuses();

    public abstract Set<ChannelDependency> getCachedChannelDependencies();

    public abstract Set<ChannelTag> getCachedChannelTags();

    public abstract String getUserTags();

    public abstract void doRefreshChannels();

    public abstract void doRefreshChannels(boolean queue);
    
    public abstract void retrieveGroups();
        
    public abstract void retrieveChannelIdsAndNames();

    public abstract void retrieveChannels();

    public abstract void retrieveChannels(boolean refreshTags);
    
    public abstract void retrieveDependencies();
    
    public abstract Map<String, ChannelHeader> getChannelHeaders();
    
    public abstract void initPanelPlugins();
    
    public abstract void closePopupWindow();
    
    public abstract void updateDefaultChannelGroup(List<DashboardStatus> statuses);
    
    public abstract List<Channel> getSelectedChannels();
    
    public abstract boolean doExportChannel();
    
    public abstract void updateChannelStatuses(List<ChannelSummary> changedChannels);
    
    public abstract void addChannelToGroup(String channelId, String groupId);
    
    public abstract void createNewChannel();
    
    public abstract void clearChannelCache();
    
    public JXTaskPane channelTasks;
    public JPopupMenu channelPopupMenu;
}
