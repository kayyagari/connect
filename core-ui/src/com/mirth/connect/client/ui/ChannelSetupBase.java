package com.mirth.connect.client.ui;

import java.util.Map;

import javax.swing.JPanel;

import com.mirth.connect.client.ui.editors.BaseEditorPaneBase;
import com.mirth.connect.model.Channel;
import com.mirth.connect.model.Connector;
import com.mirth.connect.model.Filter;
import com.mirth.connect.model.MessageStorageMode;
import com.mirth.connect.model.Rule;
import com.mirth.connect.model.Step;
import com.mirth.connect.model.Transformer;

public abstract class ChannelSetupBase extends JPanel {
    public Channel currentChannel;
    public BaseEditorPaneBase<Filter, Rule> filterPane;
    public BaseEditorPaneBase<Transformer, Step> transformerPane;

    public abstract int getDefaultQueueBufferSize();
    
    public abstract Channel getCurrentChannel();
    
    public abstract Map<Integer, Map<String, String>> getResourceIds();
    
    public abstract int getLastModelIndex();
    
    public abstract BaseEditorPaneBase<Transformer, Step> getTransformerPane();
    
    public abstract VariableList getDestinationVariableList();
    
    public abstract void decorateConnectorType(ConnectorTypeDecoration connectorTypeDecoration, boolean isDestination);

    public abstract JPanel getSourceConnectorPanel();

    public abstract void saveSourcePanel();

    public abstract void saveDestinationPanel();
    
    public abstract MessageStorageMode getMessageStorageMode();
    
    public abstract void updateQueueWarning(MessageStorageMode messageStorageMode);
    
    /** Sets the destination variable list from the transformer steps */
    public abstract void setDestinationVariableList();
    
    /**
     * Returns the required source data type of this channel.
     */
    public abstract String getRequiredInboundDataType();

    /**
     * Returns the required source data type of this channel.
     */
    public abstract String getRequiredOutboundDataType();

    /**
     * Returns the initial, or default, source inbound data type of this channel.
     */
    public abstract String getInitialInboundDataType();
    
    /**
     * Returns the required data type for the selected destination of this channel.
     */
    public abstract String getRequiredOutboundDestinationDataType();

    /**
     * Returns the initial, or default, inbound data type for the selected destination response of
     * this channel.
     */
    public abstract String getInitialInboundResponseDataType();

    /**
     * Returns the initial, or default, outbound data type for the selected destination response of
     * this channel.
     */
    public abstract String getInitialOutboundResponseDataType();
    
    /*
     * Set Data Types for source inbound and outbound which also means destination inbound
     */
    public abstract void checkAndSetSourceDataType();
    
    /**
     * Set Data types specified by selected destination for destination and response
     */
    public abstract void checkAndSetDestinationAndResponseDataType();

    /**
     * Adds a new channel that is passed in and then sets the overall panel to edit that channel.
     */
    public abstract void addChannel(Channel channel, String groupId);

    public abstract String checkInvalidPluginProperties(Channel channel);

    /**
     * Sets the overall panel to edit the channel with the given channel index. 
     */
    public abstract void editChannel(Channel channel);

    public abstract void closePopupWindow();

    /**
     * Save all of the current channel information in the editor to the actual channel
     */
    public abstract boolean saveChanges();

    /**
     * Moves the selected destination to the next spot in the array list.
     */
    public abstract void moveDestinationDown();

    /**
     * Moves the selected destination to the previous spot in the array list.
     */
    public abstract void moveDestinationUp();

    public abstract void validateScripts();

    /** 
     * Adds a new destination. 
     */
    public abstract void addNewDestination();

    /** 
     * Deletes the selected destination. 
     */
    public abstract void deleteDestination();

    public abstract void cloneDestination();

    public abstract void enableDestination();

    public abstract void disableDestination();

    /**
     * Checks all of the connectors in this channel and returns the errors found.
     * 
     * @param channel
     * @return
     */
    public abstract String checkAllForms(Channel channel);

    public abstract void setChannelEnabledField(boolean enabled);

    /**
     * Is called to load the transformer pane on either the source or destination
     */
    public abstract String editTransformer();

    /**
     * Is called to load the response transformer pane on the destination
     */
    public abstract String editResponseTransformer();

    /**
     * Is called to load the filter pane on either the source or destination 
     */
    public abstract String editFilter();

    public abstract void doValidate();

    public abstract void importConnector(Connector connector);

    public abstract Connector exportSelectedConnector();
    
    public abstract int getSelectedDestinationIndex();
}
