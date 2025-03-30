package com.mirth.connect.client.ui.codetemplate;

import java.util.Map;
import java.util.Set;

import org.jdesktop.swingx.treetable.TreeTableNode;

import com.mirth.connect.client.ui.AbstractFramePanel;
import com.mirth.connect.client.ui.ExtendedSwingWorker;
import com.mirth.connect.model.codetemplates.CodeTemplate;
import com.mirth.connect.model.codetemplates.CodeTemplateLibrary;
import com.mirth.connect.model.codetemplates.CodeTemplateLibrarySaveResult;

public abstract class CodeTemplatePanelBase extends AbstractFramePanel {

    public static final String OPTION_ONLY_SINGLE_CODE_TEMPLATES = "onlySingleCodeTemplates";
    public static final String OPTION_ONLY_SINGLE_LIBRARIES = "onlySingleLibraries";
    
    public static final String NEW_CHANNELS = "[New Channels]";
    
    public abstract Map<String, CodeTemplateLibrary> getCachedCodeTemplateLibraries();
    
    public abstract Map<String, CodeTemplate> getCachedCodeTemplates();
    
    public abstract ExtendedSwingWorker<CodeTemplateLibrarySaveResult, Void> getSwingWorker(Map<String, CodeTemplateLibrary> libraries, Map<String, CodeTemplateLibrary> removedLibraries, Map<String, CodeTemplate> updatedCodeTemplates, Map<String, CodeTemplate> removedCodeTemplates, boolean override);

    public abstract void doRefreshCodeTemplates();
    
    public abstract void doRefreshCodeTemplates(boolean showMessageOnForbidden);
    
    public abstract boolean promptSave(boolean force);
    
    public abstract String getCurrentSelectedId();
    
    public abstract CodeTemplateLibrarySaveResult attemptUpdate(Map<String, CodeTemplateLibrary> libraries, Map<String, CodeTemplateLibrary> removedLibraries, Map<String, CodeTemplate> updatedCodeTemplates, Map<String, CodeTemplate> removedCodeTemplates, boolean override, TreeTableNode selectedNode, Set<String> expandedLibraryIds);
}
