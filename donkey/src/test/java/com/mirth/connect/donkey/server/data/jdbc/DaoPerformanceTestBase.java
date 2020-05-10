package com.mirth.connect.donkey.server.data.jdbc;

import java.util.Calendar;
import java.util.UUID;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import com.mirth.connect.donkey.model.message.ContentType;
import com.mirth.connect.donkey.model.message.Message;
import com.mirth.connect.donkey.model.message.MessageContent;
import com.mirth.connect.donkey.server.data.DonkeyDao;
import com.mirth.connect.donkey.server.data.DonkeyDaoFactory;

public abstract class DaoPerformanceTestBase {
    protected static DonkeyDaoFactory factory;
    
    private static final String serverId = UUID.randomUUID().toString();
    private static final String channelId = UUID.randomUUID().toString();
    private static final long localChannelId = 1;

    @Test
    public void testInsertMessagePerf() {
        DonkeyDao dao = factory.getDao();
        long start = System.currentTimeMillis();
        int count = 100000;
        for(int i = 0; i < count; i++) {
            _insertMessage(dao);
        }
        dao.commit();
        long end = System.currentTimeMillis();
        
        System.out.println("time taken to insert " + count + " entries " + (end - start) + "msec");
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
}
