package com.mirth.connect.connectors.core.dimse;

import java.io.IOException;
import java.security.GeneralSecurityException;


public interface IMirthDcmSnd {

	public void setTlsWithoutEncyrption();
	
	public void setTls3DES_EDE_CBC();
	
	public void setTlsAES_128_CBC();
	
	public void setTrustStoreURL(String url);

	public void setTrustStorePassword(String pw);
	
	public void setKeyPassword(String pw);
	
	public void setKeyStoreURL(String url);
	
	public void setKeyStorePassword(String pw);
	
	public void setTlsNeedClientAuth(boolean needClientAuth);
	
	public void setTlsProtocol(String[] tlsProtocol);
	
	public void initTLS() throws GeneralSecurityException, IOException;
	
    public Object getDevice();

    public Object getNetworkConnection();

    public Object getRemoteNetworkConnection();

    public Object getRemoteStgcmtNetworkConnection() ;
	
}
