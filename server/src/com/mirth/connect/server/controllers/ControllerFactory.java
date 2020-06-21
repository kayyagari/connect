/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.server.controllers;

import com.google.inject.Inject;
import com.mirth.connect.donkey.server.BdbJeDataSource;
import com.mirth.connect.server.controllers.je.BdbJeControllerFactory;

public abstract class ControllerFactory {
    @Inject
    private static ControllerFactory factory;

    public static ControllerFactory getFactory() {
        synchronized (ControllerFactory.class) {
            if (factory == null) {
                if(BdbJeDataSource.getInstance() != null) {
                    factory = new BdbJeControllerFactory();
                }
                else {
                    factory = new DefaultControllerFactory();
                }
            }

            return factory;
        }
    }

    public abstract AuthorizationController createAuthorizationController();

    public abstract AlertController createAlertController();

    public abstract ChannelController createChannelController();

    public abstract CodeTemplateController createCodeTemplateController();

    public abstract ConfigurationController createConfigurationController();

    public abstract EngineController createEngineController();

    public abstract EventController createEventController();

    public abstract ExtensionController createExtensionController();

    public abstract MessageController createMessageController();

    public abstract MigrationController createMigrationController();

    public abstract ScriptController createScriptController();

    public abstract UsageController createUsageController();

    public abstract UserController createUserController();

    public abstract DatabaseTaskController createDatabaseTaskController();

    public abstract ContextFactoryController createContextFactoryController();
}
