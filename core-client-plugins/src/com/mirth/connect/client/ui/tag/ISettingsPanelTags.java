package com.mirth.connect.client.ui.tag;

import java.util.Set;

import com.mirth.connect.model.ChannelTag;

public interface ISettingsPanelTags {

    public Set<ChannelTag> getCachedChannelTags();
    
    public void refresh();
}
