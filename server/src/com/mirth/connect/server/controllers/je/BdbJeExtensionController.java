package com.mirth.connect.server.controllers.je;

import com.mirth.connect.server.ExtensionLoader;
import com.mirth.connect.server.controllers.DefaultExtensionController;
import com.mirth.connect.server.controllers.ExtensionController;

public class BdbJeExtensionController extends DefaultExtensionController {

    public static ExtensionController create() {
        synchronized (BdbJeExtensionController.class) {
            if (instance == null) {
                instance = ExtensionLoader.getInstance().getControllerInstance(ExtensionController.class);

                if (instance == null) {
                    instance = new BdbJeExtensionController();
                }
            }

            return instance;
        }
    }

    @Override
    public void uninstallExtensions() {
    }

}
