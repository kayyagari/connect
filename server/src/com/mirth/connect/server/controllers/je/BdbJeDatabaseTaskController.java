package com.mirth.connect.server.controllers.je;

import java.util.Collections;
import java.util.Map;

import com.mirth.connect.model.DatabaseTask;
import com.mirth.connect.server.ExtensionLoader;
import com.mirth.connect.server.controllers.DatabaseTaskController;

public class BdbJeDatabaseTaskController implements DatabaseTaskController {
    private static DatabaseTaskController instance;

    public static DatabaseTaskController create() {
        synchronized (BdbJeDatabaseTaskController.class) {
            if (instance == null) {
                instance = ExtensionLoader.getInstance().getControllerInstance(DatabaseTaskController.class);

                if (instance == null) {
                    instance = new BdbJeDatabaseTaskController();
                }
            }

            return instance;
        }
    }
    
    @Override
    public Map<String, DatabaseTask> getDatabaseTasks() throws Exception {
        return Collections.EMPTY_MAP;
    }

    @Override
    public String runDatabaseTask(String taskId) throws Exception {
        return "BDB JE DB controller doesn't support DB tasks";
    }

    @Override
    public void cancelDatabaseTask(String taskId) throws Exception {
    }
}
