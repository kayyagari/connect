package com.mirth.connect.donkey.server.channel;

public interface PollSourceConnectorPlugin extends SourceConnectorPlugin {

	public void poll() throws InterruptedException;
	
}
