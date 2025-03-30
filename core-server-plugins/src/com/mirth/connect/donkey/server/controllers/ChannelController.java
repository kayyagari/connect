package com.mirth.connect.donkey.server.controllers;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mirth.connect.donkey.model.channel.Ports;
import com.mirth.connect.donkey.model.message.Status;
import com.mirth.connect.donkey.server.channel.Statistics;

public abstract class ChannelController {

	public abstract void removeChannel(String channelId);

	public abstract void loadStatistics(String serverId);

	public abstract Statistics getStatistics();

	public abstract Statistics getTotalStatistics();

	public abstract Statistics getStatisticsFromStorage(String serverId);

	public abstract List<Ports> getPortsInUse();

	public abstract Statistics getTotalStatisticsFromStorage(String serverId);

	public abstract int getConnectorMessageCount(String channelId, String serverId, int metaDataId, Status status);

	public abstract void resetStatistics(Map<String, List<Integer>> channelConnectorMap, Set<Status> statuses);

	public abstract void resetAllStatistics();

	public abstract Long getLocalChannelId(String channelId);

	public abstract Long getLocalChannelId(String channelId, boolean readOnly);

	public abstract void initChannelStorage(String channelId);

	public abstract boolean channelExists(String channelId);

	public abstract void deleteAllMessages(String channelId);
	
}
