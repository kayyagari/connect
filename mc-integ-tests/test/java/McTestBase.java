package java;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mirth.connect.client.core.Client;

public class McTestBase {
    protected final static String serverUrl = "https://localhost:8443";
    protected final static String username = "admin";
    protected final static String password = username;
    
    protected static Client client;

    @BeforeClass
    public static void initClient() throws Exception {
        client = new Client(serverUrl);
        client.login(username, password);
    }

    @AfterClass
    public void closeClient() {
        client.close();
    }
    
    @Test
    public void testSendMessage() {
        
    }
}
