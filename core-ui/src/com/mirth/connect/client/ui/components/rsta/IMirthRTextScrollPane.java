package com.mirth.connect.client.ui.components.rsta;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.dnd.DropTarget;

import javax.swing.border.Border;
import javax.swing.text.Document;
import javax.swing.text.JTextComponent;

import com.mirth.connect.model.codetemplates.ContextType;

public interface IMirthRTextScrollPane {
    
    void setContextType(ContextType contextType);
    
    void setSyntaxEditingStyle(String styleKey);
    
    void setBorder(Border border);
    
    JTextComponent getTextArea();
    
    void setEditable(boolean b);
    
    void setDropTarget(DropTarget dt);
    
    Document getDocument();
    
    void setLineNumbersEnabled(boolean enabled);
    
    void setSaveEnabled(boolean saveEnabled);
    
    String getText();
    
    void setText(String text);
    
    void setText(String text, boolean discardEdits);
    
    void setToolTipText(String text);
    
    void setMinimumSize(Dimension minimumSize);
    
    void setBackground(Color bg);
    
    void setVisible(boolean aFlag);
}
