package com.mirth.connect.connectors.core.file;

import java.net.URI;
import java.net.URISyntaxException;

import com.mirth.connect.connectors.core.file.filesystems.FileSystemConnection;

public interface IFileConnector {
	
	public void releaseConnection(FileSystemConnection connection, FileSystemConnectionOptions fileSystemOptions) throws Exception;
	
	public void destroyConnection(FileSystemConnection connection, FileSystemConnectionOptions fileSystemOptions) throws Exception;
	
	public void doStop() throws FileConnectorException;
	
	public void disconnect();
	
	public FileSystemConnection getConnection(FileSystemConnectionOptions fileSystemOptions) throws Exception;
	
	public URI getEndpointURI(String host, FileScheme scheme, SchemeProperties schemeProperties, boolean isSecure) throws URISyntaxException;
	
	public String getPathPart(URI uri);
	
}
