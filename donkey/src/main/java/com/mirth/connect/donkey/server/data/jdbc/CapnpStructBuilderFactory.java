package com.mirth.connect.donkey.server.data.jdbc;

import java.nio.ByteBuffer;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.capnproto.StructFactory;

import com.mirth.connect.donkey.model.message.CapnpModel.CapAttachment;
import com.mirth.connect.donkey.model.message.CapnpModel.CapConnectorMessage;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMessage;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMetadata;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMetadataColumn;
import com.mirth.connect.donkey.model.message.CapnpModel.CapPerson;
import com.mirth.connect.donkey.model.message.CapnpModel.CapStatistics;

@SuppressWarnings("rawtypes")
public class CapnpStructBuilderFactory implements KeyedPooledObjectFactory<Class, ReusableMessageBuilder> {

    public static final int CapAttachment_Size = 5 * 1024;
    public static final int CapMessage_Size = 256;
    public static final int CapMessageContent_Size = 2 * 1024;
    public static final int CapConnectorMessage_Size = 512;
    public static final int CapMetadata_Size = 1024;
    public static final int CapMetadataColumn_Size = 256;
    public static final int CapStatistics_Size = 256;

    public static final int CapPerson_Size = 2 * 1024;

    @Override
    public void activateObject(Class key, PooledObject<ReusableMessageBuilder> po)
            throws Exception {
        //System.out.println("activating " + key + " " + po.getObject());
    }

    @Override
    public void destroyObject(Class key, PooledObject<ReusableMessageBuilder> po)
            throws Exception {
        //System.out.println("destroying " + key);
        po.getObject().reset();
    }

    @Override
    public PooledObject<ReusableMessageBuilder> makeObject(Class key)
            throws Exception {
        //System.out.println("making " + key);
        ByteBuffer buf;
        StructFactory factory;
        if(key == CapMessage.class) {
            buf = ByteBuffer.allocate(CapMessage_Size);
            factory = CapMessage.factory;
        }
        else if(key == CapAttachment.class) {
            buf = ByteBuffer.allocate(CapAttachment_Size);
            factory = CapAttachment.factory;
        }
        else if(key == CapMessageContent.class) {
            buf = ByteBuffer.allocate(CapMessageContent_Size);
            factory = CapMessageContent.factory;
        }
        else if(key == CapConnectorMessage.class) {
            buf = ByteBuffer.allocate(CapConnectorMessage_Size);
            factory = CapConnectorMessage.factory;
        }
        else if(key == CapMetadata.class) {
            buf = ByteBuffer.allocate(CapMetadata_Size);
            factory = CapMetadata.factory;
        }
        else if(key == CapMetadataColumn.class) {
            buf = ByteBuffer.allocate(CapMetadataColumn_Size);
            factory = CapMetadataColumn.factory;
        }
        else if(key == CapStatistics.class) {
            buf = ByteBuffer.allocate(CapStatistics_Size);
            factory = CapStatistics.factory;
        }
        else if(key == CapPerson.class) {
            buf = ByteBuffer.allocate(CapPerson_Size);
            factory = CapPerson.factory;
        }
        else {
            throw new IllegalArgumentException("unknown message class " + key.getName());
        }

        return new DefaultPooledObject<ReusableMessageBuilder>(new ReusableMessageBuilder(buf, factory));
    }

    @Override
    public void passivateObject(Class key, PooledObject<ReusableMessageBuilder> po)
            throws Exception {
        //System.out.println("passivating " + key + " " + po.getObject());
        po.getObject().reset();
    }

    @Override
    public boolean validateObject(Class key, PooledObject<ReusableMessageBuilder> po) {
        //System.out.println("validating " + key);
        return true;
    }
}
