package com.mirth.connect.donkey.server.data.jdbc;

import static com.mirth.connect.donkey.util.SerializerUtil.bytesToInt;
import static com.mirth.connect.donkey.util.SerializerUtil.bytesToLong;
import static com.mirth.connect.donkey.util.SerializerUtil.intToBytes;
import static com.mirth.connect.donkey.util.SerializerUtil.longToBytes;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.commons.codec.binary.Hex;
import org.capnproto.ArrayInputStream;
import org.capnproto.ArrayOutputStream;
import org.capnproto.BufferedOutputStreamWrapper;
import org.capnproto.MessageBuilder;
import org.capnproto.MessageReader;
import org.capnproto.SerializePacked;
import org.junit.Before;
import org.junit.Test;

import com.mirth.connect.donkey.model.message.CapnpModel.CapAttachment;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent;
import com.mirth.connect.donkey.model.message.Message;
import com.mirth.connect.donkey.model.message.attachment.Attachment;
import com.mirth.connect.donkey.util.CapnpInputStream;
import com.mirth.connect.donkey.util.SerializerUtil;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.SequenceConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

public class GeneralJETest {
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
        //System.out.println("id equals " + id.equals(uuid));
        
        MessageBuilder mb2 = new MessageBuilder();
        mb2.setRoot(CapAttachment.factory, atReader);
        CapAttachment.Builder cb = mb2.getRoot(CapAttachment.factory);
        System.out.println(cb.getId());
    }

    @Test
    public void testCapnpMessageSizeWithNettyByteBuf() throws Exception {
        MessageBuilder mb = new MessageBuilder();
        ByteBuffer buf = ByteBuffer.allocate(60);
        ArrayOutputStream aout = new ArrayOutputStream(buf);
        
        CapAttachment.Builder at = mb.initRoot(CapAttachment.factory);
        String uuid = UUID.randomUUID().toString();
        at.setId(uuid);
        //at.setContent("capnproto".getBytes());
        at.setType("text");
        SerializePacked.write(aout, mb);
        System.out.println(aout.getWriteBuffer().position());
        
        buf = aout.getWriteBuffer();
        int pos = buf.position();
        buf.flip();
        ArrayInputStream ai = new ArrayInputStream(buf);
        MessageReader mr = SerializePacked.read(ai);
        CapAttachment.Reader atReader= mr.getRoot(CapAttachment.factory);
        String id = atReader.getId().toString();
        System.out.println(id);
        System.out.println("id equals " + id.equals(uuid));
        assertTrue(id.equals(uuid));
        System.out.println(atReader.getContent());


        PooledByteBufAllocator bufAlloc = new PooledByteBufAllocator();
        MessageBuilder mb1 = new MessageBuilder();
        CapAttachment.Builder at1 = mb1.initRoot(CapAttachment.factory);
        at1.setId(uuid);
        at1.setType("text");
        ByteBuf buf1 = bufAlloc.buffer();
        SerializerUtil.writeMessage(mb1, buf1);
        System.out.println(buf1.readableBytes());

        assertEquals(pos, buf1.readableBytes());

        byte[] data = new byte[buf1.readableBytes()];
        System.arraycopy(buf1.array(), buf1.readerIndex(), data, 0, buf1.readableBytes());

        System.out.println(Hex.encodeHex(buf.array()));
        System.out.println(Hex.encodeHex(data));
        assertTrue(Arrays.equals(buf.array(), data));

        //ArrayInputStream ai1 = new ArrayInputStream(ByteBuffer.wrap(data));
        MessageReader mr1 = SerializerUtil.readMessage(data);
        CapAttachment.Reader atReader1 = mr1.getRoot(CapAttachment.factory);
        id = atReader1.getId().toString();
        System.out.println(id);
        System.out.println("id equals " + id.equals(uuid));
        assertTrue(id.equals(uuid));
        System.out.println(atReader1.getContent());
//        
//        MessageBuilder mb2 = new MessageBuilder();
//        mb2.setRoot(CapAttachment.factory, atReader);
//        CapAttachment.Builder cb = mb2.getRoot(CapAttachment.factory);
//        System.out.println(cb.getAttachmentSize());
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
    
    @Test
    public void testKeyRangeSearch() {
        DatabaseConfig dc = new DatabaseConfig();
        dc.setAllowCreate(true);
        Database db = env.openDatabase(null, "key_range.db", dc);
        for(long i=1; i<10; i++) {
            for(int j=0; j < 5; j++) {
                byte[] buf = new byte[12]; // MESSAGE_ID, METADATA_ID
                longToBytes(i, buf, 0);
                intToBytes(j, buf, 8);
                DatabaseEntry key = new DatabaseEntry(buf);
                DatabaseEntry data = new DatabaseEntry(longToBytes(i+1));
                db.put(null, key, data);
            }
        }
        
//        int total = deleteAllStartingwith(1, db);
//        System.out.println("total " + total);
        
        byte[] buf = new byte[12];
        longToBytes(4, buf, 0);
        DatabaseEntry key = new DatabaseEntry(buf);
        DatabaseEntry data = new DatabaseEntry();
        Cursor cursor = db.openCursor(null, null);
        OperationStatus os = cursor.getSearchKeyRange(key, data, null);
        System.out.println(os + " " + bytesToLong(key.getData()));
        longToBytes(2, buf, 0);
        key.setData(buf);
        os = cursor.getSearchKeyRange(key, data, null);
        System.out.println(os + " " + bytesToLong(key.getData()));

//        cursor.close();
//        cursor = db.openCursor(null, null);
        System.out.println("searching with zero prefix");
        longToBytes(0, buf, 0);
        intToBytes(1, buf, 8);
        key.setData(buf);
        os = cursor.getSearchKeyRange(key, data, null);
        System.out.println(os + " " + bytesToInt(key.getData(), 8) + ", data = " + bytesToLong(data.getData()));

        longToBytes(2, buf, 0);
        key.setData(buf);
        os = cursor.getNext(key, data, null);
        System.out.println(os + " " + bytesToInt(key.getData(), 8) + ", data = " + bytesToLong(data.getData()));
    }
    
    private int deleteAllStartingwith(long keyPrefixId, Database db) {
        Cursor cursor = db.openCursor(null, null);
        DatabaseEntry key = new DatabaseEntry(longToBytes(keyPrefixId));
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus os = cursor.getSearchKeyRange(key, data, null);
        int total = 0;
        if(os == OperationStatus.SUCCESS) {
            do {
                System.out.println(bytesToLong(key.getData()) + " - " + bytesToLong(data.getData()));
                long curPrefix = bytesToLong(key.getData());
                if(curPrefix != keyPrefixId) {
                    break;
                }
                cursor.delete();
                total++;
            }
            while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS);
        }
        
        cursor.close();
        
        return total;
    }
    
    @Test
    public void testDecodeMessageContent() throws Exception {
        String[] hexStrings = new String[] {
          //"101350050301013f350c29ba710101010001311d2a0131052a010000ff3437656662653231032d656431632d346438352d613264642d62386630656566640f63323531ff3835393635616463032d623133312d346330642d626434632d62356431386332330f346666390000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001010500702100101013f350c29ba710100003fa00c29ba7101000131052a0111153aff38353936", 
           "1026ff3437656662653231000101d00a642dff62386630656566640031012a08ff3c6d61703e0a20201f3c656e7472793e0a202020203c737472696e673e64313c2f737472696e673e0a202020203c726573706f6e73653e0a2020202020203c7374617475733e4552524f523c2f7374617475733e0a2020202020203c6d6573736167653e3c2f6d6573736167653e0a2020202020203c6572726f723e3c2f6572726f723e0a2020202020203c7374617475734d6573736167653e4572726f7220636f6e76657274696e67206d657373616765206f72206576616c756174696e672066696c7465722f7472616e73666f726d65723c2f7374617475734d6573736167653e0a202020203c2f726573706f6e73653e0a20203c2f656e7472793e0a3c2f0f6d61703e"
          //"101350050301013f350c29ba710101010001311d2a0131052a010000ff3437656662653231032d656431632d346438352d613264642d62386630656566640f63323531ff383539" 
        };
        
        for(String s : hexStrings) {
            byte[] data = Hex.decodeHex(s);
            ArrayInputStream in = new ArrayInputStream(ByteBuffer.wrap(data));
            MessageReader mr = SerializePacked.read(in);
            CapMessageContent.Reader mcr = mr.getRoot(CapMessageContent.factory);
            System.out.println(mcr.getMessageId());
            System.out.println(mcr.getMetaDataId());
        }
    }
}
