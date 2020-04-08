package com.mirth.connect.donkey.server.data.jdbc;

import static com.mirth.connect.donkey.util.SerializerUtil.longToBytes;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.capnproto.ArrayInputStream;
import org.capnproto.ArrayOutputStream;
import org.capnproto.BufferedOutputStreamWrapper;
import org.capnproto.MessageBuilder;
import org.capnproto.MessageReader;
import org.capnproto.SerializePacked;
import org.junit.Before;
import org.junit.Test;

import com.mirth.connect.donkey.model.message.CapnpModel.CapAttachment;
import com.mirth.connect.donkey.model.message.Message;
import com.mirth.connect.donkey.model.message.attachment.Attachment;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.SequenceConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;

public class BdbJeDaoTest {

    private Environment env;
    private EntityStore st;

    @Before
    public void setup() {
        EnvironmentConfig myEnvConfig = new EnvironmentConfig();
        StoreConfig storeConfig = new StoreConfig();

        myEnvConfig.setAllowCreate(true);
        storeConfig.setAllowCreate(true);

        // Open the environment and entity store
        File envHome = new File("/tmp/bdb");
        envHome.mkdir();
        env = new Environment(envHome, myEnvConfig);
        st = new EntityStore(env, "D_M", storeConfig);
    }

    @Test
    public void testStoreMessage() {
        PrimaryIndex pi = st.getPrimaryIndex(Long.class, Message.class);
        Message m1 = new Message();
        m1.setMessageId(1L);
        m1.setServerId(UUID.randomUUID().toString());
        m1.setChannelId(UUID.randomUUID().toString());
        m1.setImportId(1L);
        Attachment a1 = new Attachment(UUID.randomUUID().toString(), "test content attached to m1".getBytes(), "text");
        List<Attachment> lst = new ArrayList<>();
        lst.add(a1);
        m1.setAttachments(lst);
        
        pi.put(m1);
        
        Message retrieved = (Message)pi.get(1L);
        System.out.println(retrieved);
        assertNotNull(retrieved);
    }
    
    @Test
    public void testStoreCapnpMessage() throws Exception {
        MessageBuilder mb = new MessageBuilder();
        FileOutputStream fout = new FileOutputStream("/tmp/packed.dat");
        BufferedOutputStreamWrapper bw = new BufferedOutputStreamWrapper(fout.getChannel());
        
        for(int i =0; i < 100000; i++) {
            CapAttachment.Builder at = mb.initRoot(CapAttachment.factory);
            at.setId(UUID.randomUUID().toString());
            at.setContent("capnproto".getBytes());
            at.setEncrypt(false);
            at.setType("text");
            SerializePacked.write(bw, mb);
            mb.clearFirstSegment();
            System.out.println(i);
        }
        bw.flush();
        bw.close();
        fout.close();
    }

    @Test
    public void testCapnpMessageSize() throws Exception {
        MessageBuilder mb = new MessageBuilder();
        ByteBuffer buf = ByteBuffer.allocate(1024);
        ArrayOutputStream aout = new ArrayOutputStream(buf);
        
        CapAttachment.Builder at = mb.initRoot(CapAttachment.factory);
        String uuid = UUID.randomUUID().toString();
        at.setId(uuid);
        //at.setContent("capnproto".getBytes());
        at.setEncrypt(false);
        at.setType("text");
        SerializePacked.write(aout, mb);
        System.out.println(aout.getWriteBuffer().position());
        

        buf = aout.getWriteBuffer();
        buf.flip();
        ArrayInputStream ai = new ArrayInputStream(buf);
        MessageReader mr = SerializePacked.read(ai);
        CapAttachment.Reader atReader= mr.getRoot(CapAttachment.factory);
        String id = atReader.getId().toString();
        System.out.println(id);
        System.out.println("id equals " + id.equals(uuid));
        System.out.println(atReader.getEncrypt());
        System.out.println(atReader.getContent());
    }
    
    @Test
    public void testSeq() {
        SequenceConfig sc = new SequenceConfig();
        sc.setAllowCreate(true);
        sc.setInitialValue(1);
        sc.setCacheSize(1000);
        
        DatabaseConfig dc = new DatabaseConfig();
        dc.setAllowCreate(true);
        Database seDb = env.openDatabase(null, "seq_db", dc);
        DatabaseEntry key = new DatabaseEntry(longToBytes(1));
        Sequence s = seDb.openSequence(null, key, sc);
        
        long start = System.currentTimeMillis();
        for(int i = 0; i < 10; i++) {
            long l = s.get(null, 1);
            System.out.println(l);
        }
        long end = System.currentTimeMillis();
        
        long l = s.get(null, 1);
        System.out.println(l);
        s.close();
        seDb.close();
        
        System.out.println("\ntime taken " + (end-start) + "msec");
    }
}
