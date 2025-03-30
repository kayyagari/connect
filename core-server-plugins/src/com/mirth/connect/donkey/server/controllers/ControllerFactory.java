package com.mirth.connect.donkey.server.controllers;

public abstract class ControllerFactory {
	
	private static final String FACTORY_CLASS_NAME = "com.mirth.connect.donkey.server.controllers.DefaultControllerFactory";
	private static ControllerFactory factory;
	
	public static ControllerFactory getFactory() {
		synchronized (ControllerFactory.class) {
			if (factory == null) {
                try {
                    factory = (ControllerFactory) Class.forName(FACTORY_CLASS_NAME).newInstance();
                } catch (Throwable t) {
                    t.printStackTrace();
                }
			}
			
			return factory;
		}
	}

	public abstract ChannelController createChannelController();
	
	public abstract MessageController createMessageController();
	
}
