package com.mirth.connect.plugins.core.httpauth;

import java.util.List;
import java.util.Map;

import com.mirth.connect.plugins.core.httpauth.userutil.AuthStatus;

public abstract class AuthenticationResultBase {

	public abstract AuthStatus getStatus();

	public abstract Map<String, List<String>> getResponseHeaders();

	public abstract String getUsername();

	public abstract String getRealm();
	


}
