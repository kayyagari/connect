package com.mirth.connect.connectors.core.http;

import java.io.IOException;
import java.util.Enumeration;

import javax.servlet.ServletInputStream;

public interface IRequest {
	
	Object getRequest();
	
	Object getAttribute(String name);
	
	void setAttribute(String name, Object value);
	
	String getContentType();
	
	String getHeader(String name);
	
	Enumeration<String> getHeaders(String name);
	
	Enumeration<String> getHeaderNames();
	
	ServletInputStream getInputStream() throws IOException;
	
	String getMethod();
	
	String getRequestURI();
	
	StringBuffer getRequestURL();
	
	String getScheme();
	
}
