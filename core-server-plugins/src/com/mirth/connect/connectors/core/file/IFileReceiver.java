package com.mirth.connect.connectors.core.file;

import com.mirth.connect.donkey.server.channel.ISourceConnector;

public interface IFileReceiver extends ISourceConnector {

	public void setFileConnector(IFileConnector fileConnector);
	
}
