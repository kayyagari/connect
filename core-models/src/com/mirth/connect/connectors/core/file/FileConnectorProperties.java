package com.mirth.connect.connectors.core.file;

public interface FileConnectorProperties {

	public String getHost();
	
	public FileScheme getScheme();
	
	public SchemeProperties getSchemeProperties();
	
	public String getTimeout();
	
	public String getUsername();
	
	public String getPassword();
	
	public boolean isAnonymous();
	
	public boolean isPassive();
	
	public boolean isSecure();
	
	public boolean isValidateConnection();
	
}
