package com.mirth.connect.connectors.http;

import java.io.IOException;
import java.util.Enumeration;

import javax.servlet.ServletInputStream;

import org.eclipse.jetty.server.Request;

import com.mirth.connect.connectors.core.http.IRequest;

public class RequestWrapper implements IRequest {
	
	private Request request;
	
	public RequestWrapper(Request request) {
		this.request = request;
	}
	
	@Override
	public Object getRequest() {
		return request;
	}
	
	@Override
	public Object getAttribute(String name) {
		return request.getAttribute(name);
	}

	@Override
	public void setAttribute(String name, Object value) {
		request.setAttribute(name, value);
	}
	
	@Override
	public String getContentType() {
		return request.getContentType();
	}
	
	@Override
	public String getHeader(String name) {
		return request.getHeader(name);
	}
	
	@Override
	public Enumeration<String> getHeaders(String name) {
		return request.getHeaders(name);
	}
	
	@Override
	public Enumeration<String> getHeaderNames() {
		return request.getHeaderNames();
	}
	
	@Override
	public ServletInputStream getInputStream() throws IOException {
		return request.getInputStream();
	}

	@Override
	public String getMethod() {
		return request.getMethod();
	}

	@Override
	public String getRequestURI() {
		return request.getRequestURI();
	}
	
	@Override
	public StringBuffer getRequestURL() {
		return request.getRequestURL();
	}
	
	@Override
	public String getScheme() {
		return request.getScheme();
	}

}
