package com.mirth.connect.server.util.javascript;

import java.util.Map;
import java.util.Set;

import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.Scriptable;

import com.mirth.connect.donkey.server.ConnectorTaskException;
import com.mirth.connect.model.codetemplates.ContextType;
import com.mirth.connect.util.ClassUtil;

/***
 * This class gives access to methods from JavaScript util classes in the Server without
 * needing to move those whole classes into the core libraries.
 */
public class JavaScriptCoreUtil {

	public static Class<?> JAVASCRIPT_UTIL_CLASS;
	public static Class<?> JAVASCRIPT_SCOPE_UTIL_CLASS;
	
	/*
	 * JavaScriptUtil methods
	 */
	public static <T> T execute(JavaScriptTask<T> task) throws JavaScriptExecutorException, InterruptedException {
		IJavaScriptUtil jsUtil = ClassUtil.createInstance(IJavaScriptUtil.class, JavaScriptCoreUtil.JAVASCRIPT_UTIL_CLASS);
		return jsUtil.doExecute(task);
	}
	
	public static IMirthContextFactory generateContextFactory(boolean debug, Set<String> libraryResourceIds, String channelId, String scriptId, String script, ContextType contextType) throws ConnectorTaskException {
		IJavaScriptUtil jsUtil = ClassUtil.createInstance(IJavaScriptUtil.class, JavaScriptCoreUtil.JAVASCRIPT_UTIL_CLASS);
		return jsUtil.doGenerateContextFactory(debug, libraryResourceIds, channelId, scriptId, script, contextType);
	}
	
	public static boolean recompileGeneratedScript(IMirthContextFactory contextFactory, String scriptId) throws Exception {
		IJavaScriptUtil jsUtil = ClassUtil.createInstance(IJavaScriptUtil.class, JavaScriptCoreUtil.JAVASCRIPT_UTIL_CLASS);
		return jsUtil.doRecompileGeneratedScript(contextFactory, scriptId);
	}
	
	/*
	 * JavaScriptScopeUtil methods
	 */
	public static Context getContext(ContextFactory contextFactory) {
		IJavaScriptScopeUtil jsUtil = ClassUtil.createInstance(IJavaScriptScopeUtil.class, JavaScriptCoreUtil.JAVASCRIPT_SCOPE_UTIL_CLASS);
		return jsUtil.doGetContext(contextFactory);
	}
	
	public static Scriptable getBatchProcessorScope(ContextFactory contextFactory, Object logger, String channelId, String channelName, Map<String, Object> scopeObjects) {
		IJavaScriptScopeUtil jsUtil = ClassUtil.createInstance(IJavaScriptScopeUtil.class, JavaScriptCoreUtil.JAVASCRIPT_SCOPE_UTIL_CLASS);
		return jsUtil.doGetBatchProcessorScope(contextFactory, logger, channelId, channelName, scopeObjects);
	}
	
}
