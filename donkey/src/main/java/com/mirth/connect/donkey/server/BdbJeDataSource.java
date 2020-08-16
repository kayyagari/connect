package com.mirth.connect.donkey.server;

import java.io.File;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

import com.mirth.connect.donkey.model.DatabaseConstants;
import com.mirth.connect.donkey.server.data.jdbc.CapnpStructBuilderFactory;
import com.mirth.connect.donkey.server.data.jdbc.ReusableMessageBuilder;
import com.mirth.connect.donkey.util.SerializerUtil;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
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

    private DatabaseConfig defaultDbConf;
    private SequenceConfig defaultSeqConf;

    private static final String TABLE_SERVER_SEQ = "server_seq";
    public static final String TABLE_D_MESSAGE_SEQ = "d_msq";

    private static BdbJeDataSource instance;

    private BdbJeDataSource() {
        defaultDbConf = new DatabaseConfig();
        // the below two config options are same for all Databases
        // and MUST be set or overwritten
        defaultDbConf.setTransactional(true);
        defaultDbConf.setAllowCreate(true);

        defaultSeqConf = new SequenceConfig();
        defaultSeqConf.setAllowCreate(true);
        defaultSeqConf.setInitialValue(1);
        // there is an issue with any size larger than 1 for caching
        // after a restart the next id returned is at cacheSize + 1
        // instead of being at the last used value + 1
        defaultSeqConf.setCacheSize(1);
        defaultSeqConf.setAutoCommitNoSync(true);
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
                if(!envDir.exists()) {
                    envDir.mkdirs();
                }
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
                instance.loadDatabaseHandles();
                instance.createServerControllerDatabases();
            }
        }
        
        return instance;
    }

    private void createServerControllerDatabases() {
        String[] tableNames = {"person", "code_template_library", "channel_group", 
                "code_template", "channel", "configuration", "script", "alert", "event", TABLE_SERVER_SEQ};

        if(bdbJeEnv.getDatabaseNames().contains(tableNames[0])) {
            return;
        }
        Transaction txn = bdbJeEnv.beginTransaction(null, null);
        for(String name : tableNames) {
            Database db = bdbJeEnv.openDatabase(txn, name, defaultDbConf);
            dbMap.put(name, db);
        }
        
        Database serverSeqDb = dbMap.get(TABLE_SERVER_SEQ);
        createSequence("person", serverSeqDb, txn);
        createSequence("event", serverSeqDb, txn);
        
        txn.commit();
    }

    private void createSequence(String seqKey, Database seqDb, Transaction txn) {
        DatabaseEntry key = new DatabaseEntry(seqKey.getBytes(UTF_8));
        Sequence seq = seqDb.openSequence(txn, key, defaultSeqConf);
        serverSeqMap.put(seqKey, seq);
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

    public Database truncateAndReopen(Database db) {
        String name = db.getDatabaseName();
        synchronized(name) {
            db.close();
            Transaction txn = bdbJeEnv.beginTransaction(null, null);
            bdbJeEnv.truncateDatabase(txn, name, false);
            txn.commit();
            
            txn = bdbJeEnv.beginTransaction(null, null);
            db = bdbJeEnv.openDatabase(txn, name, defaultDbConf);
            txn.commit();
            dbMap.put(name, db);
            
            return db;
        }
    }

    public GenericKeyedObjectPool<Class, ReusableMessageBuilder> getObjectPool() {
        return objectPool;
    }

    public GenericKeyedObjectPool<Class, ReusableMessageBuilder> getServerObjectPool() {
        return serverObjectPool;
    }
    
    private void loadDatabaseHandles() {
        List<String> lst = bdbJeEnv.getDatabaseNames();
        Transaction txn = bdbJeEnv.beginTransaction(null, null);
        for(String name : lst) {
            Database db = bdbJeEnv.openDatabase(txn, name, defaultDbConf);
            dbMap.put(name, db);
        }
        
        Database channelSeqTbl = dbMap.get(TABLE_D_MESSAGE_SEQ);
        Function channelFunc = new Function<DatabaseEntry, Void>() {
            @Override
            public Void apply(DatabaseEntry key) {
                Sequence seq = channelSeqTbl.openSequence(txn, key, defaultSeqConf);
                long id = SerializerUtil.bytesToLong(key.getData());
                channelSeqMap.put(id, seq);
                return null;
            }
        };
        loadSequences(txn, channelSeqTbl, channelFunc);

        Database serverSeqTbl = dbMap.get(TABLE_SERVER_SEQ);
        Function serverFunc = new Function<DatabaseEntry, Void>() {
            @Override
            public Void apply(DatabaseEntry key) {
                Sequence seq = serverSeqTbl.openSequence(txn, key, defaultSeqConf);
                String id = new String(key.getData(), UTF_8);
                serverSeqMap.put(id, seq);
                return null;
            }
        };
        loadSequences(txn, serverSeqTbl, serverFunc);

        txn.commit();
    }
    
    public SequenceConfig getDefaultSeqConf() {
        return defaultSeqConf;
    }

    private void loadSequences(Transaction txn, Database tbl, Function<DatabaseEntry, Void> f) {
        if(tbl != null) {
            Cursor cursor = tbl.openCursor(null, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS) {
                f.apply(key);
            }
            cursor.close();
        }
    }
}
