/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.client.ui.alert;

import java.util.List;
import java.util.Map;

import javax.swing.JPanel;

import com.mirth.connect.model.alert.AlertActionGroup;

@SuppressWarnings("serial")
public abstract class AlertActionPaneBase extends JPanel{
	
	public abstract void setActionGroup(AlertActionGroup actionGroupTableModel, Map<String, Map<String, String>> protocolOptions);
	
	public abstract void setVariableList(List<String> variables);

	public AlertActionPaneBase createNewPanel() {
		return null;
	}

}
