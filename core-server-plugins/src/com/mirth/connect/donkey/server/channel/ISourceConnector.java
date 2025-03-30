package com.mirth.connect.donkey.server.channel;

import com.mirth.connect.donkey.model.channel.DeployedState;
import com.mirth.connect.donkey.model.message.BatchRawMessage;
import com.mirth.connect.donkey.model.message.RawMessage;
import com.mirth.connect.donkey.server.ConnectorTaskException;
import com.mirth.connect.donkey.server.message.batch.BatchAdaptorFactory;
import com.mirth.connect.donkey.server.message.batch.BatchMessageException;
import com.mirth.connect.donkey.server.message.batch.ResponseHandler;

public interface ISourceConnector extends IConnector {
	
	public void initialize(SourceConnectorPlugin connectorPlugin);
	
    public void doStart() throws ConnectorTaskException, InterruptedException;

    public void doStop() throws ConnectorTaskException, InterruptedException;

    public void doHalt() throws ConnectorTaskException, InterruptedException;
	
    public boolean isRespondAfterProcessing();
    
    public void setRespondAfterProcessing(boolean respondAfterProcessing);
	
	public Object getMetaDataReplacer();
	
	public void setMetaDataReplacer(Object metaDataReplacer);
	
    public BatchAdaptorFactory getBatchAdaptorFactory();

    public void setBatchAdaptorFactory(BatchAdaptorFactory batchAdaptorFactory);
	
	public String getSourceName();
	
	public void setSourceName(String sourceName);
	
	public DeployedState doGetCurrentState();
	
	public void updateCurrentState(DeployedState currentState);
	
	public void doUpdateCurrentState(DeployedState currentState);

	public boolean isProcessBatch();
	
	public DispatchResult dispatchRawMessage(RawMessage rawMessage) throws ChannelException;
	
	public Boolean dispatchBatchMessage(BatchRawMessage batchRawMessage, ResponseHandler responseHandler) throws BatchMessageException;
	
	public void finishDispatch(DispatchResult dispatchResult);
	
}
