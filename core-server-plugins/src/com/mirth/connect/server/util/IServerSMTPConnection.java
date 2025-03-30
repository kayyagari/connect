package com.mirth.connect.server.util;

public interface IServerSMTPConnection {

	public void setHost(String replaceValues);

	public void setPort(String string);

	public void setSocketTimeout(int parseInt);

	public void setUseAuthentication(boolean smtpAuth);

	public void setUsername(String replaceValues);

	public void setFrom(String replaceValues);

	public void setPassword(String replaceValues);

	public void setSecure(String smtpSecure);

	public void send(String join, String object, String subject, String body) throws Exception ;

}
