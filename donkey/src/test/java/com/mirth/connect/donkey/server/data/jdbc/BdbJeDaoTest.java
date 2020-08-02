package com.mirth.connect.donkey.server.data.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mirth.connect.donkey.model.channel.MetaDataColumn;
import com.mirth.connect.donkey.model.channel.MetaDataColumnType;
import com.mirth.connect.donkey.model.message.ConnectorMessage;
import com.mirth.connect.donkey.model.message.Message;
import com.mirth.connect.donkey.model.message.attachment.Attachment;
import com.mirth.connect.donkey.server.BdbJeDataSource;
import com.mirth.connect.donkey.server.Donkey;
import com.mirth.connect.donkey.server.DonkeyConfiguration;
import com.mirth.connect.donkey.server.StartException;
import com.mirth.connect.donkey.server.channel.Channel;
import com.mirth.connect.donkey.server.data.DonkeyDao;
import com.mirth.connect.donkey.server.data.DonkeyDaoFactory;
import com.mirth.connect.donkey.test.util.TestUtils;
import com.mirth.connect.donkey.util.Serializer;

public class BdbJeDaoTest {

    private static DonkeyDaoFactory factory;

    private static final String serverId = "85965adc-b131-4c0d-bd4c-b5d18c234ff9";
    private static final String channelId = "47efbe21-ed1c-4d85-a2dd-b8f0eefdc251";
    private static final String channelName = "bdbje-test";
    private static final long localChannelId = 1;

    private static final Map<Long, Message> injectedMessages = new HashMap<>();
    private static final List<MetaDataColumn> metaDataColumns = new ArrayList<>();

    private DonkeyDao dao;

    static {
        metaDataColumns.add(new MetaDataColumn("DRAW_TIME", MetaDataColumnType.TIMESTAMP, "DRAW_TIME"));
        metaDataColumns.add(new MetaDataColumn("HASEDGES", MetaDataColumnType.BOOLEAN, "HASEDGES"));
        metaDataColumns.add(new MetaDataColumn("MESSAGE_ID", MetaDataColumnType.NUMBER, "MESSAGE_ID"));
        metaDataColumns.add(new MetaDataColumn("SOURCE", MetaDataColumnType.STRING, "SOURCE"));
        metaDataColumns.add(new MetaDataColumn("METADATA_ID", MetaDataColumnType.NUMBER, "METADATA_ID"));
        metaDataColumns.add(new MetaDataColumn("HEIGHT", MetaDataColumnType.NUMBER, "HEIGHT"));
        metaDataColumns.add(new MetaDataColumn("TYPE", MetaDataColumnType.STRING, "TYPE"));
    }

    @BeforeClass
    final public static void beforeClass() throws Exception {
        Donkey jeDonkey = Donkey.getInstance();
        DonkeyConfiguration dconf = TestUtils.getDonkeyTestConfigurationForJE(true);
        dconf.setServerId(serverId);
        jeDonkey.startEngine(dconf);
        
        Channel channel = new Channel();
        channel.setChannelId(channelId);
        channel.setName(channelName);
        channel.setLocalChannelId(localChannelId);
        jeDonkey.getDeployedChannels().put(channelId, channel);

        List<String> dbNames = jeDonkey.getBdbJeEnv().getDatabaseNames();
        assertTrue(dbNames.contains(BdbJeDao.TABLE_D_CHANNELS));
        assertTrue(dbNames.contains(BdbJeDataSource.TABLE_D_MESSAGE_SEQ));
        assertTrue(dbNames.contains(BdbJeDao.TABLE_D_META_COLUMNS));

        factory = jeDonkey.getDaoFactory();
        DonkeyDao dao = factory.getDao();
        dao.createChannel(channelId, localChannelId);
        dao.commit();
        
        dbNames = jeDonkey.getBdbJeEnv().getDatabaseNames();
        assertTrue(dbNames.contains("d_m" + localChannelId));
        assertTrue(dbNames.contains("d_mm" + localChannelId));
        assertTrue(dbNames.contains("d_mm_status" + localChannelId));
        assertTrue(dbNames.contains("d_mc" + localChannelId));
        assertTrue(dbNames.contains("d_mcm" + localChannelId));
        assertTrue(dbNames.contains("d_ma" + localChannelId));
        assertTrue(dbNames.contains("d_ms" + localChannelId));
        
        injectMessages();
    }

    @AfterClass
    final public static void afterClass() throws StartException {
        Donkey.getInstance().stopEngine();
    }

    private static void injectMessages() throws Exception {
        Serializer s = Donkey.getInstance().getSerializer();
        for(int i = 1; i <= 3; i++ ) {
            InputStream in = BdbJeDaoTest.class.getResourceAsStream("sample-messages/message_" + i + ".xml");
            StringWriter sw = new StringWriter();
            IOUtils.copy(in, sw);
            Message m = s.deserialize(sw.toString(), Message.class);
            
            // input validity checks
            assertEquals(serverId, m.getServerId());
            assertEquals(channelId, m.getChannelId());

            injectedMessages.put(m.getMessageId(), m);
            DonkeyDao dao = factory.getDao();
            
            dao.insertMessage(m);
            if(m.getAttachments() != null) {
                for(Attachment a : m.getAttachments()) {
                    dao.insertMessageAttachment(channelId, m.getMessageId(), a);
                }
            }
            for(ConnectorMessage cm : m.getConnectorMessages().values()) {
                dao.insertConnectorMessage(cm, true, true);
                dao.insertMetaData(cm, metaDataColumns);
            }
            
            dao.commit();
        }
    }

    @Before
    final public void before() {
        dao = factory.getDao();
    }
    
    @After
    final public void after() {
        dao.commit();
    }

    // injectMessages() already inserted Message and ConnectorMessage objects
    // test the retrieval here
    @Test
    public void testMessageInsert() {
        List<Message> lst = dao.getMessages(channelId, new ArrayList<Long>(injectedMessages.keySet()));
        assertEquals(injectedMessages.size(), lst.size());
        for(Message clone : lst) {
            Message original = injectedMessages.get(clone.getMessageId());
            assertNotNull(original);
            if(original.getAttachments() != null) {
                List<Attachment> attachments = dao.getMessageAttachment(channelId, original.getMessageId());
                clone.setAttachments(attachments);
            }
            boolean result = original.equals(clone);
            System.out.println("compared message " + clone.getMessageId() + " " + result);
            assertTrue(result);
        }
    }
}
