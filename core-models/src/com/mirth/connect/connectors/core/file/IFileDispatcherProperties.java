package com.mirth.connect.connectors.core.file;

public interface IFileDispatcherProperties extends FileConnectorProperties {

	public boolean isKeepConnectionOpen();
	
	public String getMaxIdleTime();
	
}
