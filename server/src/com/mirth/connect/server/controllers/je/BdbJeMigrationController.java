package com.mirth.connect.server.controllers.je;

import org.apache.commons.configuration.PropertiesConfiguration;

import com.mirth.connect.model.util.MigrationException;
import com.mirth.connect.server.ExtensionLoader;
import com.mirth.connect.server.controllers.MigrationController;

public class BdbJeMigrationController extends MigrationController {
    private static MigrationController instance = null;

    public static MigrationController create() {
        synchronized (BdbJeMigrationController.class) {
            if (instance == null) {
                instance = ExtensionLoader.getInstance().getControllerInstance(MigrationController.class);

                if (instance == null) {
                    instance = new BdbJeMigrationController();
                }
            }

            return instance;
        }
    }

    @Override
    public void migrate() throws MigrationException {
    }

    @Override
    public void migrateSerializedData() {
    }

    @Override
    public void migrateExtensions() {
    }

    @Override
    public void migrateConfiguration(PropertiesConfiguration configuration)
            throws MigrationException {
    }
}
