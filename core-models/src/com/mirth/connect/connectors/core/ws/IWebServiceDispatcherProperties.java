package com.mirth.connect.connectors.core.ws;

import java.util.List;
import java.util.Map;

public interface IWebServiceDispatcherProperties {
	
	public String getWsdlUrl();
	
	public void setWsdlUrl(String wsdlUrl);
	
	public String getService();
	
	public void setService(String service);
	
	public String getPort();
	
	public void setPort(String port);
	
	public String getLocationURI();

	public void setLocationURI(String locationURI);
	
	public String getSocketTimeout();
	
	public void setSocketTimeout(String socketTimeout);
	
    public String getOperation();

    public void setOperation(String operation);
	
    public boolean isUseAuthentication();

    public void setUseAuthentication(boolean useAuthentication);
	
    public String getUsername();
    
    public void setUsername(String username);
    
    public String getPassword();
    
    public void setPassword(String password);
    
    public String getEnvelope();
    
    public void setEnvelope(String envelope);
    
    public boolean isOneWay();

    public void setOneWay(boolean oneWay); 
    
    public String getHeadersVariable();

    public void setHeadersVariable(String headersVariable);

    public boolean isUseHeadersVariable();

    public void setUseHeadersVariable(boolean isUseHeadersVariable);
    		
    public Map<String, List<String>> getHeadersMap();
    
    public void setHeadersMap(Map<String, List<String>> headers);
    
    public boolean isUseMtom();

    public void setUseMtom(boolean useMtom);
    
    public String getAttachmentsVariable();
    
    public void setAttachmentsVariable(String attachmentsVariable);
    		
	public List<String> getAttachmentNames();
	
	public void setAttachmentNames(List<String> attachmentNames);
	
    public List<String> getAttachmentContents();

    public void setAttachmentContents(List<String> attachmentContents);
    
	public List<String> getAttachmentTypes();
	
	public void setAttachmentTypes(List<String> attachmentTypes);
	
	public boolean isUseAttachmentsVariable();
	
	public void setUseAttachmentsVariable(boolean isUseAttachmentsVariable);
	
	public String getSoapAction();
	
	public void setSoapAction(String soapAction);
	
    public DefinitionServiceMap getWsdlDefinitionMap();

    public void setWsdlDefinitionMap(DefinitionServiceMap wsdlDefinitionMap);
	
}
