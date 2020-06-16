package com.mirth.connect.server.controllers;

import java.io.File;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;

import com.mirth.connect.donkey.server.Donkey;
import com.mirth.connect.donkey.server.DonkeyConfiguration;
import com.mirth.connect.donkey.test.util.TestUtils;
import com.mirth.connect.server.controllers.je.BdbJeUserController;
import com.mirth.connect.server.migration.ResourceUtil;

public class BdbJeUserControllerTests extends UserControllerTests {
    private static final String serverId = "85965adc-b131-4c0d-bd4c-b5d18c234ff9";
    private static final String channelId = "47efbe21-ed1c-4d85-a2dd-b8f0eefdc251";

    private static Donkey jeDonkey;
    
    static {
        try {
            String jks = "keystore.jks";
            InputStream stream = ResourceUtil.getResourceStream(BdbJeUserControllerTests.class, jks);
            File appdata = new File("appdata");
            appdata.mkdir();
            FileUtils.copyInputStreamToFile(stream, new File(appdata, jks));
            stream.close();
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void setupDb() throws Exception {
        if(jeDonkey == null) {
            jeDonkey = Donkey.getInstance();
            DonkeyConfiguration dconf = TestUtils.getDonkeyTestConfigurationForJE(true);
            dconf.setServerId(serverId);
            jeDonkey.startEngine(dconf);
        }
        ConfigurationController cc = ControllerFactory.getFactory().createConfigurationController();
        cc.initializeSecuritySettings();
    }

    @Override
    protected void setupController() throws Exception {
        BdbJeUserController jeController = (BdbJeUserController)BdbJeUserController.create();
        jeController._removeAllUsers();
        userController = jeController;
    }
}
