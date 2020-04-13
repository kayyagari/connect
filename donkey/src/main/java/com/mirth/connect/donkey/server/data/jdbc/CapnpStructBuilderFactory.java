package com.mirth.connect.donkey.server.data.jdbc;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.capnproto.MessageBuilder;
import org.capnproto.StructBuilder;

import com.mirth.connect.donkey.model.message.CapnpModel.CapAttachment;
import com.mirth.connect.donkey.model.message.CapnpModel.CapConnectorMessage;
import com.mirth.connect.donkey.model.message.CapnpModel.CapErrorContent;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMapContent;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMessage;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMetadata;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMetadataColumn;

@SuppressWarnings("rawtypes")
public class CapnpStructBuilderFactory implements KeyedPooledObjectFactory<Class, ReusableMessageBuilder> {

    @Override
    public void activateObject(Class key, PooledObject<ReusableMessageBuilder> po)
            throws Exception {
    }

    @Override
    public void destroyObject(Class key, PooledObject<ReusableMessageBuilder> po)
            throws Exception {
        po.getObject().reset();
    }

    @Override
    public PooledObject<ReusableMessageBuilder> makeObject(Class key)
            throws Exception {
        MessageBuilder mb = new MessageBuilder();
        StructBuilder sb = null;
        
        if(key == CapMessage.class) {
            sb = mb.initRoot(CapMessage.factory);
        }
        else if(key == CapAttachment.class) {
            sb = mb.initRoot(CapAttachment.factory);
        }
        else if(key == CapMessageContent.class) {
            sb = mb.initRoot(CapMessageContent.factory);
        }
        else if(key == CapConnectorMessage.class) {
            sb = mb.initRoot(CapConnectorMessage.factory);
        }
        else if(key == CapMapContent.class) {
            sb = mb.initRoot(CapMapContent.factory);
        }
        else if(key == CapErrorContent.class) {
            sb = mb.initRoot(CapErrorContent.factory);
        }
        else if(key == CapMetadata.class) {
            sb = mb.initRoot(CapMetadata.factory);
        }
        else if(key == CapMetadataColumn.class) {
            sb = mb.initRoot(CapMetadataColumn.factory);
        }
        else {
            throw new IllegalArgumentException("unknown message class " + key.getName());
        }

        return new DefaultPooledObject<ReusableMessageBuilder>(new ReusableMessageBuilder(mb, sb));
    }

    @Override
    public void passivateObject(Class key, PooledObject<ReusableMessageBuilder> po)
            throws Exception {
        po.getObject().reset();
    }

    @Override
    public boolean validateObject(Class key, PooledObject<ReusableMessageBuilder> po) {
        return true;
    }
}
