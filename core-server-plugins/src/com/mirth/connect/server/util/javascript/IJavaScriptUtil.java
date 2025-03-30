package com.mirth.connect.server.util.javascript;

import java.util.Set;

import com.mirth.connect.donkey.server.ConnectorTaskException;
import com.mirth.connect.model.codetemplates.ContextType;

public interface IJavaScriptUtil {

	public <T> T doExecute(JavaScriptTask<T> task) throws JavaScriptExecutorException, InterruptedException;
	
	public IMirthContextFactory doGenerateContextFactory(boolean debug, Set<String> libraryResourceIds, String channelId, String scriptId, String script, ContextType contextType) throws ConnectorTaskException;
	
	public boolean doRecompileGeneratedScript(IMirthContextFactory contextFactory, String scriptId) throws Exception;
	
}
