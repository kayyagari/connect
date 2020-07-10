package com.mirth.connect.server.controllers.je;

import com.mirth.connect.server.controllers.AlertController;
import com.mirth.connect.server.controllers.AuthorizationController;
import com.mirth.connect.server.controllers.ChannelController;
import com.mirth.connect.server.controllers.CodeTemplateController;
import com.mirth.connect.server.controllers.ConfigurationController;
import com.mirth.connect.server.controllers.ContextFactoryController;
import com.mirth.connect.server.controllers.ControllerFactory;
import com.mirth.connect.server.controllers.DatabaseTaskController;
import com.mirth.connect.server.controllers.DefaultAuthorizationController;
import com.mirth.connect.server.controllers.DefaultChannelController;
import com.mirth.connect.server.controllers.DefaultCodeTemplateController;
import com.mirth.connect.server.controllers.DefaultContextFactoryController;
import com.mirth.connect.server.controllers.DefaultDatabaseTaskController;
import com.mirth.connect.server.controllers.DefaultEventController;
import com.mirth.connect.server.controllers.DefaultExtensionController;
import com.mirth.connect.server.controllers.DefaultMigrationController;
import com.mirth.connect.server.controllers.DefaultScriptController;
import com.mirth.connect.server.controllers.DefaultUsageController;
import com.mirth.connect.server.controllers.DefaultUserController;
import com.mirth.connect.server.controllers.DonkeyEngineController;
import com.mirth.connect.server.controllers.DonkeyMessageController;
import com.mirth.connect.server.controllers.EngineController;
import com.mirth.connect.server.controllers.EventController;
import com.mirth.connect.server.controllers.ExtensionController;
import com.mirth.connect.server.controllers.MessageController;
import com.mirth.connect.server.controllers.MigrationController;
import com.mirth.connect.server.controllers.ScriptController;
import com.mirth.connect.server.controllers.UsageController;
import com.mirth.connect.server.controllers.UserController;

public class BdbJeControllerFactory extends ControllerFactory {
    public AuthorizationController createAuthorizationController() {
        return DefaultAuthorizationController.create();
    }

    public AlertController createAlertController() {
        return BdbJeAlertController.create();
    }

    public ChannelController createChannelController() {
        return BdbJeChannelController.create();
    }

    public CodeTemplateController createCodeTemplateController() {
        return BdbJeCodeTemplateController.create();
    }

    public ConfigurationController createConfigurationController() {
        return BdbJeConfigurationController.create();
    }

    public EngineController createEngineController() {
        return DonkeyEngineController.getInstance();
    }

    public EventController createEventController() {
        return BdbJeEventController.create();
    }

    public ExtensionController createExtensionController() {
        return DefaultExtensionController.create();
    }

    public MessageController createMessageController() {
        return BdbJeMessageController.create();
    }

    public MigrationController createMigrationController() {
        return BdbJeMigrationController.create();
    }

    public ScriptController createScriptController() {
        return BdbJeScriptController.create();
    }

    public UsageController createUsageController() {
        return DefaultUsageController.create();
    }

    public UserController createUserController() {
        return BdbJeUserController.create();
    }

    public DatabaseTaskController createDatabaseTaskController() {
        return BdbJeDatabaseTaskController.create();
    }

    public ContextFactoryController createContextFactoryController() {
        return DefaultContextFactoryController.create();
    }
}
