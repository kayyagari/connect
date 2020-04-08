package com.mirth.connect.donkey.server.data.jdbc;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.capnproto.MessageBuilder;
import org.capnproto.StructBuilder;

import com.mirth.connect.donkey.model.message.CapnpModel.CapMessage;

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
