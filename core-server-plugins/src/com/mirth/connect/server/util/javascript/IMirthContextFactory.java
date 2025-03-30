package com.mirth.connect.server.util.javascript;

import org.mozilla.javascript.ScriptableObject;

public interface IMirthContextFactory {
	
	public String getId();
	
	public ScriptableObject getSealedSharedScope();

}
