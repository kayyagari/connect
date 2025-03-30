package com.mirth.connect.connectors.core.smtp;

public interface ISmtpDispatcherProperties {

	public String getEncryption();
	
	public String getSmtpHost();
	
	public String getSmtpPort();
	
	public String getTimeout();
	
	public boolean isAuthentication();
	
	public String getUsername();
	
	public String getPassword();
	
	public String getTo();
	
	public String getFrom();
	
}
