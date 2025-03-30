package com.mirth.connect.donkey.server.channel;

import java.util.List;
import java.util.Set;

import com.mirth.connect.donkey.model.channel.DebugOptions;
import com.mirth.connect.donkey.model.channel.MetaDataColumn;
import com.mirth.connect.donkey.util.MessageMaps;

public interface IChannel {
	
	public String getChannelId();
	
	public void setChannelId(String channelId);
	
	public List<MetaDataColumn> getMetaDataColumns();
	
	public ISourceConnector getSourceConnector();

	public DebugOptions getDebugOptions();
	
	public Set<String> getResourceIds();
	
	public Object getStorageSettings();
	
	public Object getDaoFactory();
	
	public MessageMaps getMessageMaps();
	
}
