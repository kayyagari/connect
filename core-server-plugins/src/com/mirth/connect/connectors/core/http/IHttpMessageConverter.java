package com.mirth.connect.connectors.core.http;

import org.apache.http.entity.ContentType;

public interface IHttpMessageConverter {

	String doContentToXml(Object content, ContentType contentType, boolean parseMultipart, BinaryContentTypeResolver resolver) throws Exception;
	
}
