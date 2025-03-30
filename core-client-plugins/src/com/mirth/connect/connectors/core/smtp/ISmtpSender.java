package com.mirth.connect.connectors.core.smtp;

import javax.swing.JLabel;

import com.mirth.connect.client.ui.components.MirthRadioButton;

public interface ISmtpSender {

	public JLabel getEncryptionLabel();

	public MirthRadioButton getEncryptionNone();

	public MirthRadioButton getEncryptionTls();

	public MirthRadioButton getEncryptionSsl();
	
}
