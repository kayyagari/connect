package com.mirth.connect.donkey.server.controllers;

public class DefaultControllerFactory extends ControllerFactory {
	
	@Override
	public ChannelController createChannelController() {
		return DefaultChannelController.getInstance();
	}
	
	@Override
	public MessageController createMessageController() {
		return DefaultMessageController.getInstance();
	}
	
}
