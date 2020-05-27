package com.mirth.connect.donkey.server.data.jdbc;

import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mirth.connect.donkey.model.message.ConnectorMessage;
import com.mirth.connect.donkey.model.message.ContentType;
import com.mirth.connect.donkey.model.message.Message;
import com.mirth.connect.donkey.model.message.MessageContent;
import com.mirth.connect.donkey.server.Donkey;
import com.mirth.connect.donkey.server.DonkeyConfiguration;
import com.mirth.connect.donkey.server.DonkeyConnectionPools;
import com.mirth.connect.donkey.server.channel.Channel;
import com.mirth.connect.donkey.server.data.DonkeyDao;
import com.mirth.connect.donkey.server.data.DonkeyDaoFactory;
import com.mirth.connect.donkey.test.util.TestUtils;
import com.mirth.connect.donkey.util.Serializer;

public class DaoPerformanceTestBase {
    protected static DonkeyDaoFactory factory;
    
    private static final String serverId = UUID.randomUUID().toString();
    private static final String channelId = UUID.randomUUID().toString();
    private static final long localChannelId = 1;
    private static final String channelName = "bdbje-test";

    private static Message realMessage;

    @BeforeClass
    final public static void beforeClass() throws Exception {
        Donkey jeDonkey = Donkey.getInstance();
        DonkeyConfiguration dconf = TestUtils.getDonkeyTestConfigurationForJE(true);
        dconf.setServerId(serverId);
//        dconf = TestUtils.getDonkeyTestConfiguration();
//        DonkeyConnectionPools.getInstance().init(dconf.getDonkeyProperties());
        jeDonkey.startEngine(dconf);
        
        Channel channel = new Channel();
        channel.setChannelId(channelId);
        channel.setName(channelName);
        channel.setLocalChannelId(localChannelId);
        jeDonkey.getDeployedChannels().put(channelId, channel);

        factory = jeDonkey.getDaoFactory();
        DonkeyDao dao = factory.getDao();
        dao.createChannel(channelId, localChannelId);
        dao.commit();        

        Serializer s = Donkey.getInstance().getSerializer();
        InputStream in = BdbJeDaoTest.class.getResourceAsStream("sample-messages/message_1.xml");
        StringWriter sw = new StringWriter();
        IOUtils.copy(in, sw);
        realMessage = s.deserialize(sw.toString(), Message.class);
    }

    @Test
    public void testInsertMessagePerf() {
        DonkeyDao dao = factory.getDao();
        long start = System.currentTimeMillis();
        int count = 100000;
        for(int i = 0; i < count; i++) {
            //_insertMessage(dao);
            _insertRealMessage(dao);
        }
        dao.commit();
        long end = System.currentTimeMillis();
        
        System.out.println("time taken to insert " + count + " entries " + (end - start) + "msec");
        testReadMessagePerf();
    }

    private void testReadMessagePerf() {
        DonkeyDao dao = factory.getDao();
        int count = 1000;
        List<Long> mids = new ArrayList<>();
        for(int i = 1; i <= count; i++) {
            mids.add((long)i);
        }

        long start = System.currentTimeMillis();
        List<Message> lst = dao.getMessages(channelId, mids);
        dao.commit();
        assertEquals(count, lst.size());
        long end = System.currentTimeMillis();
        
        System.out.println("time taken to read " + count + " entries " + (end - start) + "msec");
    }

    @Test
    public void testInsertMessageContentPerf() {
        DonkeyDao dao = factory.getDao();
        long start = System.currentTimeMillis();
        int count = 100000;
        for(int i = 0; i < count; i++) {
            _insertMessageContent(dao);
        }
        dao.commit();
        long end = System.currentTimeMillis();
        
        System.out.println("time taken to insert " + count + " entries " + (end - start) + "msec");
    }

    private void _insertMessageContent(DonkeyDao dao) {
        MessageContent mc = new MessageContent();
        mc.setChannelId(channelId);
        mc.setContent(RandomStringUtils.random(100));
        mc.setContentType(ContentType.RAW);
        mc.setDataType("text");
        mc.setEncrypted(false);
        mc.setMessageId(dao.getNextMessageId(channelId));
        mc.setMetaDataId(1);
    }
    
    private void _insertMessage(DonkeyDao dao) {
        Message msg = new Message();
        msg.setChannelId(channelId);
        msg.setMessageId(dao.getNextMessageId(channelId));
        Calendar now = Calendar.getInstance();
        now.setTimeInMillis(System.currentTimeMillis());
        msg.setReceivedDate(now);
        msg.setServerId(serverId);
        dao.insertMessage(msg);
    }

    private void _insertRealMessage(DonkeyDao dao) {
        realMessage.setChannelId(channelId);
        realMessage.setMessageId(dao.getNextMessageId(channelId));
        Calendar now = Calendar.getInstance();
        now.setTimeInMillis(System.currentTimeMillis());
        realMessage.setReceivedDate(now);
        realMessage.setServerId(serverId);
        dao.insertMessage(realMessage);
        long mid = realMessage.getMessageId();
        for(ConnectorMessage cm : realMessage.getConnectorMessages().values()) {
            cm.setChannelId(channelId);
            cm.setChannelName(channelName);
            cm.setMessageId(mid);
            dao.insertConnectorMessage(cm, true, true);
        }
    }
}
