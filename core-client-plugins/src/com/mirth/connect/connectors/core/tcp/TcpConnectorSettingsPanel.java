package com.mirth.connect.connectors.core.tcp;

import com.mirth.connect.client.ui.components.MirthRadioButton;

public interface TcpConnectorSettingsPanel {
	
	public MirthRadioButton getModeServerRadio();
	
	public MirthRadioButton getModeClientRadio();
	
}
