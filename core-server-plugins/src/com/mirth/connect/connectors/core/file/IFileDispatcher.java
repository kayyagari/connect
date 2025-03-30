package com.mirth.connect.connectors.core.file;

import com.mirth.connect.donkey.server.channel.IDestinationConnector;

public interface IFileDispatcher extends IDestinationConnector {

	public void setFileConnector(IFileConnector fileConnector);
	
}
