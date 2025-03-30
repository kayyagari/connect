package com.mirth.connect.donkey.server.channel;

import java.util.List;

import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.donkey.model.channel.DeployedState;
import com.mirth.connect.donkey.model.channel.MetaDataColumn;
import com.mirth.connect.donkey.model.message.ConnectorMessage;
import com.mirth.connect.donkey.model.message.ResponseValidator;
import com.mirth.connect.donkey.model.message.Status;
import com.mirth.connect.donkey.model.message.attachment.AttachmentHandlerProvider;
import com.mirth.connect.donkey.server.ConnectorTaskException;
import com.mirth.connect.donkey.util.MessageMaps;
import com.mirth.connect.donkey.util.Serializer;

public interface IDestinationConnector extends IConnector {
	
	public void initialize(DestinationConnectorPlugin connectorPlugin);
	
    public void doStart() throws ConnectorTaskException, InterruptedException;
    
    public void doStartQueue();

    public void doStop() throws ConnectorTaskException, InterruptedException;

    public void doHalt() throws ConnectorTaskException, InterruptedException;
	
	public void doTransform(Object dao, ConnectorMessage message, Status previousStatus, boolean initialAttempt) throws InterruptedException;
	
	public void doProcess(Object dao, ConnectorMessage message, Status previousStatus) throws InterruptedException;
	
	public void doUpdateQueuedStatus(Object daoObj, ConnectorMessage message, Status previousStatus) throws InterruptedException;
	
	public void doProcessPendingConnectorMessage(Object daoObj, ConnectorMessage message) throws InterruptedException;
	
	public Object getQueue();
	
	public void setQueue(Object queue);

	public int getPotentialThreadCount();
	
	public String getDestinationName();
	
	public void setDestinationName(String destinationName);
	
	public boolean isEnabled();
	
	public void setEnabled(boolean enabled);
	
	public boolean isForceQueue();
	
	public Integer getOrderId();
	
	public void setOrderId(Integer orderId);
	
	public Serializer getSerializer();
	
	public MessageMaps getMessageMaps();
	
	public void setMetaDataReplacer(Object metaDataReplacer);
	
	public void setMetaDataColumns(List<MetaDataColumn> metaDataColumns);
	
	public ResponseValidator getResponseValidator();
	
	public void setResponseValidator(ResponseValidator responseValidator);
	
	public Object getResponseTransformerExecutor();
	
	public void setResponseTransformerExecutor(Object responseTransformerExecutor);
	
	public void setStorageSettings(Object storageSettings);
	
	public void setDaoFactory(Object daoFactory);
	
	public boolean isQueueRotate();
	
	public boolean willAttemptSend();
	
	public boolean includeFilterTransformerInQueue();
	
	public AttachmentHandlerProvider getAttachmentHandlerProvider();
	
    public void doReplaceConnectorProperties(ConnectorProperties connectorProperties, ConnectorMessage message);
    
    public void setForceQueue(boolean forceQueue);
    
    public void doSetForceQueue(boolean forceQueue);
    
    public DeployedState doGetCurrentState();
    
    public void updateCurrentState(DeployedState currentState);
    
    public void doUpdateCurrentState(DeployedState currentState);
    
    public void doRun();
    
}
