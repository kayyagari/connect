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

import javax.swing.JPanel;
import javax.swing.table.DefaultTableModel;

import com.mirth.connect.client.ui.components.MirthTextArea;
import com.mirth.connect.model.alert.AlertTrigger;

@SuppressWarnings("serial")
public abstract class AlertTriggerPane extends JPanel {

    public abstract List<String> getVariables();

    public abstract AlertTrigger getTrigger();

    public abstract void setTrigger(AlertTrigger trigger);

    public abstract void reset();

    public abstract List<String> doValidate();
    
    //channel error trigger pane
    public abstract MirthTextArea getRegexTextArea();
	
}
