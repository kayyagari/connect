package com.mirth.connect.donkey.server.data.jdbc;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.capnproto.MessageBuilder;

public class CapnpMessageBuilderFactory implements KeyedPooledObjectFactory<Class, MessageBuilder> {

    @Override
    public void activateObject(Class key, PooledObject<MessageBuilder> po)
            throws Exception {
    }

    @Override
    public void destroyObject(Class key, PooledObject<MessageBuilder> po)
            throws Exception {
        // TODO Auto-generated method stub
        
    }

    @Override
    public PooledObject<MessageBuilder> makeObject(Class key)
            throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void passivateObject(Class key, PooledObject<MessageBuilder> po)
            throws Exception {
        // TODO Auto-generated method stub
        
    }

    @Override
    public boolean validateObject(Class key, PooledObject<MessageBuilder> po) {
        // TODO Auto-generated method stub
        return false;
    }
}
