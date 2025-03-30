package com.mirth.connect.connectors.core.http;

import javax.swing.JLabel;
import javax.swing.JTextField;

import com.mirth.connect.client.ui.components.MirthTextField;
import com.mirth.connect.connectors.core.ConnectorSettingsPanelBase;

public interface IHttpListener extends ConnectorSettingsPanelBase {

	void doInitComponents();
	
	JLabel getContextPathLabel();
	
	MirthTextField getContextPathField();
	
	JLabel getReceiveTimeoutLabel();
	
	MirthTextField getReceiveTimeoutField();
	
	JLabel getHttpUrlLabel();
	
	JTextField getHttpUrlField();

}
