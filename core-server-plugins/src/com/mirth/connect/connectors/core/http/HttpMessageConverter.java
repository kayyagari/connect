package com.mirth.connect.connectors.core.http;

import org.apache.http.entity.ContentType;

import com.mirth.connect.util.ClassUtil;

public class HttpMessageConverter {
	
	public static Class<?> HTTP_MESSAGE_CONVERTER_CLASS;

	public static String contentToXml(Object content, ContentType contentType, boolean parseMultipart, BinaryContentTypeResolver resolver) throws Exception {
		IHttpMessageConverter converter = ClassUtil.createInstance(IHttpMessageConverter.class, HttpMessageConverter.HTTP_MESSAGE_CONVERTER_CLASS);
		return converter.doContentToXml(content, contentType, parseMultipart, resolver);
	}
	
}
