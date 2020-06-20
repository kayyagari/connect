package com.mirth.connect.donkey.server;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

import com.mirth.connect.donkey.model.DatabaseConstants;
import com.mirth.connect.donkey.server.data.jdbc.CapnpStructBuilderFactory;
import com.mirth.connect.donkey.server.data.jdbc.ReusableMessageBuilder;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.SequenceConfig;
import com.sleepycat.je.Transaction;

import io.netty.buffer.PooledByteBufAllocator;

public class BdbJeDataSource {
    private Environment bdbJeEnv;
    private Map<String, Database> dbMap;
    private Map<Long, Sequence> channelSeqMap;
    private Map<String, Sequence> serverSeqMap;
    private PooledByteBufAllocator bufAlloc;
    private GenericKeyedObjectPool<Class, ReusableMessageBuilder> objectPool;
    private GenericKeyedObjectPool<Class, ReusableMessageBuilder> serverObjectPool;
    
    public static final String DB_BDB_JE = "bdbje";
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private static BdbJeDataSource instance;

    private BdbJeDataSource() {
    }

    public static BdbJeDataSource getInstance() {
        return instance;
    }

    public static BdbJeDataSource create(Properties dbProperties) {
        if(instance != null) {
            return instance;
        }

        String database = dbProperties.getProperty(DatabaseConstants.DATABASE);
        if(!DB_BDB_JE.equalsIgnoreCase(database)) {
            return null;
        }
        
        synchronized (BdbJeDataSource.class) {
            if(instance == null) {
                File envDir = new File((String)dbProperties.get(DatabaseConstants.DATABASE_URL));
                
                EnvironmentConfig ec = new EnvironmentConfig();
                ec.setAllowCreate(true);
                ec.setTransactional(true);
                ec.setDurability(Durability.COMMIT_WRITE_NO_SYNC);
                //ec.setCachePercent(70);

                instance = new BdbJeDataSource();
                
                instance.bdbJeEnv = new Environment(envDir, ec);
                instance.dbMap = new ConcurrentHashMap<>();
                instance.channelSeqMap = new ConcurrentHashMap<>();
                instance.serverSeqMap = new ConcurrentHashMap<>();
                instance.bufAlloc = new PooledByteBufAllocator();
                instance.objectPool = new GenericKeyedObjectPool<Class, ReusableMessageBuilder>(new CapnpStructBuilderFactory());
                instance.serverObjectPool = new GenericKeyedObjectPool<Class, ReusableMessageBuilder>(new CapnpStructBuilderFactory());
                instance.createServerControllerDatabases();
            }
        }
        
        return instance;
    }

    private void createServerControllerDatabases() {
        Transaction txn = bdbJeEnv.beginTransaction(null, null);
        DatabaseConfig dc = new DatabaseConfig();
        // the below two config options are same for all Databases
        // and MUST be set or overwritten
        dc.setTransactional(true);
        dc.setAllowCreate(true);
        
        String[] tableNames = {"person", "code_template_library", "channel_group", 
                "code_template", "channel", "configuration", "script", "alert", "server_seq"};
        for(String name : tableNames) {
            Database db = bdbJeEnv.openDatabase(txn, name, dc);
            dbMap.put(name, db);
        }
        
        String personSeqKey = "person";
        DatabaseEntry key = new DatabaseEntry(personSeqKey.getBytes(UTF_8));
        SequenceConfig seqConfig = new SequenceConfig();
        seqConfig.setAllowCreate(true);
        seqConfig.setInitialValue(1);
        seqConfig.setCacheSize(50);

        Sequence seq = dbMap.get("server_seq").openSequence(txn, key, seqConfig);
        serverSeqMap.put(personSeqKey, seq);
        
        txn.commit();
    }

    public void close() {
        for(Sequence s : serverSeqMap.values()) {
            s.close();
        }
        for(Sequence s : channelSeqMap.values()) {
            s.close();
        }
        
        for(Database db : dbMap.values()) {
            db.close();
        }
        
        bdbJeEnv.close();
        objectPool.close();
        serverObjectPool.close();
    }

    public Environment getBdbJeEnv() {
        return bdbJeEnv;
    }

    public Map<String, Database> getDbMap() {
        return dbMap;
    }

    public Map<Long, Sequence> getSeqMap() {
        return channelSeqMap;
    }

    public Map<String, Sequence> getServerSeqMap() {
        return serverSeqMap;
    }

    public PooledByteBufAllocator getBufAlloc() {
        return bufAlloc;
    }

    public GenericKeyedObjectPool<Class, ReusableMessageBuilder> getObjectPool() {
        return objectPool;
    }

    public GenericKeyedObjectPool<Class, ReusableMessageBuilder> getServerObjectPool() {
        return serverObjectPool;
    }
}
