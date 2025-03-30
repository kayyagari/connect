package com.mirth.connect.connectors.core.ws;

import java.awt.Color;
import java.awt.Component;

import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JScrollPane;

import com.mirth.connect.client.ui.ConnectorTypeDecoration;
import com.mirth.connect.client.ui.FrameBase;
import com.mirth.connect.client.ui.components.MirthComboBox;
import com.mirth.connect.client.ui.components.MirthEditableComboBox;
import com.mirth.connect.client.ui.components.MirthIconTextField;
import com.mirth.connect.client.ui.components.MirthPasswordField;
import com.mirth.connect.client.ui.components.MirthRadioButton;
import com.mirth.connect.client.ui.components.MirthSyntaxTextArea;
import com.mirth.connect.client.ui.components.MirthTable;
import com.mirth.connect.client.ui.components.MirthTextField;
import com.mirth.connect.connectors.core.ConnectorSettingsPanelBase;

public interface IWebServiceSender extends ConnectorSettingsPanelBase {
	
	static final ImageIcon ICON_LOCK_X = new ImageIcon(FrameBase.class.getResource("images/lock_x.png"));
    static final Color COLOR_SSL_NOT_CONFIGURED = new Color(0xFFF099);
    static final String SSL_TOOL_TIP = "<html>The default system certificate store will be used for this connection.<br/>As a result, certain security options are not available and mutual<br/>authentication (two-way authentication) is not supported.</html>";

    void doInitComponents();
    
    void doInitLayout();
    
    DefinitionServiceMap getCurrentServiceMap();
	
	void setCurrentServiceMap(DefinitionServiceMap serviceMap);
	
	boolean isUsingHttps(String url);
	
	ConnectorTypeDecoration doGetConnectorTypeDecoration();
	
	void doDoLocalDecoration(ConnectorTypeDecoration connectorTypeDecoration);
	
	boolean doCanSetLocationURI();
	
	void loadServiceMap();
	
	void generateEnvelope(String wsdlUrl, String channelId, String channelName, boolean buildOptional);
	
	void doSetSoapEnvelopeText(String text);
	
	void doInitToolTips();
	
	IWebServiceDispatcherProperties doGetTestConnectionProperties();
	
	void updateGenerateEnvelopeButtonEnabled();
	
	JLabel getWsdlUrlLabel();
    
    MirthIconTextField getWsdlUrlField();

    JButton getGetOperationsButton();

	JButton getWsdlUrlTestConnectionButton();

	JLabel getServiceLabel();
    
	MirthEditableComboBox getServiceComboBox();

	JLabel getPortLabel();
    
	MirthEditableComboBox getPortComboBox();

	JLabel getLocationURILabel();
    
    MirthEditableComboBox getLocationURIComboBox();

	JButton getLocationURITestConnectionButton();

	JLabel getSocketTimeoutLabel();
    
    MirthTextField getSocketTimeoutField();

	JLabel getAuthenticationLabel();

	MirthRadioButton getAuthenticationYesRadio();

	MirthRadioButton getAuthenticationNoRadio();

	JLabel getUsernameLabel();

	MirthTextField getUsernameField();

	JLabel getPasswordLabel();

	MirthPasswordField getPasswordField();

	JLabel getInvocationTypeLabel();

	MirthRadioButton getInvocationOneWayRadio();

	MirthRadioButton getInvocationTwoWayRadio();

	JLabel getOperationLabel();
    
    MirthComboBox getOperationComboBox();

	JButton getGenerateEnvelopeButton();

	JLabel getSoapActionLabel();

	MirthIconTextField getSoapActionField();

	JLabel getSoapEnvelopeLabel();
    
    MirthSyntaxTextArea getSoapEnvelopeTextArea();

	JLabel getHeadersLabel();

	MirthTable getHeadersTable();

	JScrollPane getHeadersScrollPane();

	JButton getHeadersNewButton();

	JButton getHeadersDeleteButton();

	MirthTextField getHeadersVariableField();

	MirthRadioButton getUseHeadersTableRadio();

	MirthRadioButton getUseHeadersVariableRadio();

	JLabel getUseMtomLabel();

	MirthRadioButton getUseMtomYesRadio();

	MirthRadioButton getUseMtomNoRadio();

	JLabel getAttachmentsLabel();

	MirthTable getAttachmentsTable();

	JScrollPane getAttachmentsScrollPane();

	JButton getAttachmentsNewButton();

	JButton getAttachmentsDeleteButton();
    
    MirthTextField getAttachmentsVariableField();

	MirthRadioButton getUseAttachmentsTableRadio();

	MirthRadioButton getUseAttachmentsVariableRadio();
    
    Component getSslWarningPanel();
    
}
