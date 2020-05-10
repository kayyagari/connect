package com.mirth.connect.donkey.server.data.jdbc;

import static org.junit.Assert.*;

import java.io.InputStream;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.capnproto.ArrayInputStream;
import org.capnproto.ArrayOutputStream;
import org.capnproto.MessageReader;
import org.capnproto.SerializePacked;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mirth.connect.donkey.model.message.CapnpModel.CapMessage;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent.CapContentType;
import com.mirth.connect.donkey.model.message.ConnectorMessage;
import com.mirth.connect.donkey.model.message.MapContent;
import com.mirth.connect.donkey.model.message.Message;
import com.mirth.connect.donkey.util.MapUtil;
import com.mirth.connect.donkey.util.Serializer;
import com.mirth.connect.donkey.util.SerializerUtil;
import com.mirth.connect.donkey.util.xstream.XStreamSerializer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

public class SerializerUtilTest {
    private GenericKeyedObjectPool<Class, ReusableMessageBuilder> objectPool = new GenericKeyedObjectPool<Class, ReusableMessageBuilder>(new CapnpStructBuilderFactory());;
    private PooledByteBufAllocator bufAlloc = new PooledByteBufAllocator();
    private Serializer serializer = new XStreamSerializer();

    @Before
    public void borrowObj() throws Exception {
    }
    
    @After
    public void returnObj() {
    }

    @Test
    public void testPacked() throws Exception {
        for(int i = 1; i <= 7; i++ ) {
            InputStream in = BdbJeDaoTest.class.getResourceAsStream("sample-messages/message_" + i + ".xml");
            StringWriter sw = new StringWriter();
            IOUtils.copy(in, sw);
            Message message = serializer.deserialize(sw.toString(), Message.class);
            
            ReusableMessageBuilder rmb = objectPool.borrowObject(CapMessage.class);
            CapMessage.Builder mb = (CapMessage.Builder)rmb.getSb();
            mb.setChannelId(message.getChannelId());
            if(message.getImportChannelId() != null) {
                mb.setImportChannelId(message.getImportChannelId());
            }
            if(message.getImportId() != null) {
                mb.setImportId(message.getImportId());
            }
            mb.setMessageId(message.getMessageId());
            if(message.getOriginalId() != null) {
                mb.setOriginalId(message.getOriginalId());
            }
            mb.setProcessed(message.isProcessed());
            mb.setReceivedDate(message.getReceivedDate().getTimeInMillis());
            mb.setServerId(message.getServerId());
            
            //byte[] dataS = packWithUtil(rmb);
            byte[] dataC = packWithCapnp(rmb);
            objectPool.returnObject(CapMessage.class, rmb);
            //assertTrue(Arrays.equals(dataS, dataC));
            
            for(ConnectorMessage cm : message.getConnectorMessages().values()) {
                MapContent mc = cm.getSourceMapContent();
                Map<String, Object> map = mc.getMap();
                String content = null;
                if (MapUtils.isNotEmpty(map)) {
                    content = MapUtil.serializeMap(serializer, map);
                }
                
                rmb = objectPool.borrowObject(CapMessageContent.class);
                CapMessageContent.Builder cb = (CapMessageContent.Builder) rmb.getSb();
                cb.setContent(content);
                cb.setContentType(CapContentType.SOURCEMAP);
                cb.setDataType("map");
                cb.setEncrypted(false);
                cb.setMessageId(message.getMessageId());
                cb.setMetaDataId(cm.getMetaDataId());
                
                //dataS = packWithUtil(rmb);
                dataC = packWithCapnp(rmb);
                objectPool.returnObject(CapMessageContent.class, rmb);
//            System.out.println(Hex.encodeHexString(dataS));
//            System.out.println(Hex.encodeHexString(dataC));
            System.out.println(i + " " + message.getMessageId() + " " + cm.getMetaDataId() + " " + dataC.length);
//            assertEquals("packed message content lengths are not equal", dataS.length, dataC.length);
//            assertTrue(Arrays.equals(dataS, dataC));
                MessageReader mr = readWithCapnp(dataC);
                CapMessageContent.Reader cr = mr.getRoot(CapMessageContent.factory);
                System.out.println(cr.getContentType());
            }
        }
    }

    private byte[] packWithUtil(ReusableMessageBuilder rmb) throws Exception {
        ByteBuf byteBuf = bufAlloc.buffer(10240);
        SerializerUtil.writeMessage(rmb.getMb(), byteBuf);
        byte[] data = new byte[byteBuf.readableBytes()];
        System.arraycopy(byteBuf.array(), byteBuf.readerIndex(), data, 0, byteBuf.readableBytes());
        return data;
    }
    
    private byte[] packWithCapnp(ReusableMessageBuilder rmb) throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(10 * 1024);
        ArrayOutputStream output = new ArrayOutputStream(buf);
        SerializePacked.write(output, rmb.getMb());
        buf = output.getWriteBuffer();
        buf.flip();
        byte[] data = new byte[buf.remaining()];
        System.arraycopy(buf.array(), 0, data, 0, buf.remaining());
        return data;
    }
    
    private MessageReader readWithCapnp(byte[] data) throws Exception {
        ArrayInputStream in = new ArrayInputStream(ByteBuffer.wrap(data));
        return SerializePacked.read(in);
    }
}
