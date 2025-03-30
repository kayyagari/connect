package com.mirth.connect.server.util.javascript;

import java.util.Map;

import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.Scriptable;

public interface IJavaScriptScopeUtil {

	public Context doGetContext(ContextFactory contextFactory);
	
	public Scriptable doGetBatchProcessorScope(ContextFactory contextFactory, Object logger, String channelId, String channelName, Map<String, Object> scopeObjects);
	
}
