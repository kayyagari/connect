/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.donkey.server;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.log4j.Logger;

import com.google.inject.Inject;
import com.mirth.connect.donkey.model.DatabaseConstants;
import com.mirth.connect.donkey.server.channel.Channel;
import com.mirth.connect.donkey.server.controllers.ChannelController;
import com.mirth.connect.donkey.server.data.DonkeyDao;
import com.mirth.connect.donkey.server.data.DonkeyDaoFactory;
import com.mirth.connect.donkey.server.data.DonkeyStatisticsUpdater;
import com.mirth.connect.donkey.server.data.jdbc.BdbJeDaoFactory;
import com.mirth.connect.donkey.server.data.jdbc.CapnpStructBuilderFactory;
import com.mirth.connect.donkey.server.data.jdbc.JdbcDaoFactory;
import com.mirth.connect.donkey.server.data.jdbc.ReusableMessageBuilder;
import com.mirth.connect.donkey.server.data.jdbc.XmlQuerySource;
import com.mirth.connect.donkey.server.data.jdbc.XmlQuerySource.XmlQuerySourceException;
import com.mirth.connect.donkey.server.event.EventDispatcher;
import com.mirth.connect.donkey.util.Serializer;
import com.mirth.connect.donkey.util.SerializerProvider;
import com.mirth.connect.donkey.util.xstream.XStreamSerializer;
import com.sleepycat.je.Database;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Sequence;

import io.netty.buffer.PooledByteBufAllocator;

public class Donkey {
    @Inject
    private static Donkey instance;

    public static Donkey getInstance() {
        synchronized (Donkey.class) {
            if (instance == null) {
                instance = new Donkey();
            }

            return instance;
        }
    }

    public Donkey() {

    }

    private Map<String, Channel> deployedChannels = new ConcurrentHashMap<String, Channel>();
    private DonkeyConfiguration donkeyConfiguration;
    private DonkeyDaoFactory daoFactory;
    private DonkeyDaoFactory readOnlyDaoFactory;
    private Serializer serializer = new XStreamSerializer();
    private Encryptor encryptor;
    private EventDispatcher eventDispatcher;
    private DonkeyStatisticsUpdater statisticsUpdater;
    private Logger logger = Logger.getLogger(getClass());
    private boolean running = false;

    private Environment bdbJeEnv;
    private Map<String, Database> dbMap;
    private Map<Long, Sequence> seqMap;
    private PooledByteBufAllocator bufAlloc;
    private GenericKeyedObjectPool<Class, ReusableMessageBuilder> objectPool;
    
    private static final String DB_BDB_JE = "bdbje";

    public void startEngine(DonkeyConfiguration donkeyConfiguration) throws StartException {
        this.donkeyConfiguration = donkeyConfiguration;

        Properties dbProperties = donkeyConfiguration.getDonkeyProperties();
        String database = dbProperties.getProperty(DatabaseConstants.DATABASE);

        SerializerProvider serializerProvider = new SerializerProvider() {
            @Override
            public Serializer getSerializer(Integer metaDataId) {
                return serializer;
            }
        };

        if(DB_BDB_JE.equalsIgnoreCase(database)) {
            initBdbJeEnv(dbProperties);
            BdbJeDaoFactory bdbJedaoFactory = BdbJeDaoFactory.getInstance();
            bdbJedaoFactory.setStatsServerId(donkeyConfiguration.getServerId());
            daoFactory = bdbJedaoFactory;
        }
        else {
            XmlQuerySource xmlQuerySource = new XmlQuerySource();
            
            try {
                xmlQuerySource.load("default.xml");
                xmlQuerySource.load(dbProperties.getProperty("database") + ".xml");
            } catch (XmlQuerySourceException e) {
                throw new StartException(e);
            }
            
            daoFactory = createDaoFactory(database, serializerProvider, xmlQuerySource, false);
            
            boolean splitReadWrite = Boolean.parseBoolean(dbProperties.getProperty(DatabaseConstants.DATABASE_ENABLE_READ_WRITE_SPLIT));
            
            if (splitReadWrite) {
                String readOnlyDatabase = dbProperties.getProperty(DatabaseConstants.DATABASE_READONLY, database);
                readOnlyDaoFactory = createDaoFactory(readOnlyDatabase, serializerProvider, xmlQuerySource, true);
            } else {
                readOnlyDaoFactory = daoFactory;
            }
        }

        DonkeyDao dao = null;
        try {
            dao = daoFactory.getDao();

            dao.initTableStructure();
            dao.checkAndCreateChannelTables();

            dao.commit();
        } catch (Exception e) {
            logger.error("Could not check and create channel tables on startup", e);
        } finally {
            if (dao != null) {
                dao.close();
            }
        }

        // load channel statistics into memory
        ChannelController.getInstance().loadStatistics(donkeyConfiguration.getServerId());

        encryptor = donkeyConfiguration.getEncryptor();

        eventDispatcher = donkeyConfiguration.getEventDispatcher();

        int updateInterval = NumberUtils.toInt(donkeyConfiguration.getDonkeyProperties().getProperty("donkey.statsupdateinterval"), DonkeyStatisticsUpdater.DEFAULT_UPDATE_INTERVAL);
        statisticsUpdater = new DonkeyStatisticsUpdater(daoFactory, updateInterval);
        statisticsUpdater.start();

        running = true;
    }

    private JdbcDaoFactory createDaoFactory(String database, SerializerProvider serializerProvider, XmlQuerySource xmlQuerySource, boolean readOnly) throws StartException {
        JdbcDaoFactory jdbcDaoFactory = JdbcDaoFactory.getInstance(database);
        jdbcDaoFactory.setStatsServerId(donkeyConfiguration.getServerId());

        if (readOnly) {
            jdbcDaoFactory.setConnectionPool(DonkeyConnectionPools.getInstance().getReadOnlyConnectionPool());
        } else {
            jdbcDaoFactory.setConnectionPool(DonkeyConnectionPools.getInstance().getConnectionPool());
        }

        jdbcDaoFactory.setSerializerProvider(serializerProvider);
        jdbcDaoFactory.setQuerySource(xmlQuerySource);

        return jdbcDaoFactory;
    }

    public DonkeyDaoFactory getDaoFactory() {
        return daoFactory;
    }

    public void setDaoFactory(DonkeyDaoFactory daoFactory) {
        this.daoFactory = daoFactory;
    }

    public DonkeyDaoFactory getReadOnlyDaoFactory() {
        return readOnlyDaoFactory;
    }

    public void setReadOnlyDaoFactory(DonkeyDaoFactory readOnlyDaoFactory) {
        this.readOnlyDaoFactory = readOnlyDaoFactory;
    }

    public DonkeyStatisticsUpdater getStatisticsUpdater() {
        return statisticsUpdater;
    }

    public void stopEngine() {
        if (statisticsUpdater != null) {
            statisticsUpdater.shutdown();
        }

        running = false;
    }

    public Map<String, Channel> getDeployedChannels() {
        return deployedChannels;
    }

    public Set<String> getDeployedChannelIds() {
        List<Channel> channels = new ArrayList<Channel>(deployedChannels.values());

        Collections.sort(channels, new Comparator<Channel>() {

            @Override
            public int compare(Channel o1, Channel o2) {
                return o1.getDeployDate().compareTo(o2.getDeployDate());
            }

        });

        Set<String> channelIds = new LinkedHashSet<String>();

        for (Channel channel : channels) {
            channelIds.add(channel.getChannelId());
        }

        return channelIds;
    }

    public boolean isRunning() {
        return running;
    }

    public DonkeyConfiguration getConfiguration() {
        return donkeyConfiguration;
    }

    public Serializer getSerializer() {
        return serializer;
    }

    public void setSerializer(Serializer serializer) {
        this.serializer = serializer;
    }

    public Encryptor getEncryptor() {
        return encryptor;
    }

    public EventDispatcher getEventDispatcher() {
        return eventDispatcher;
    }
    
    private void initBdbJeEnv(Properties dbProperties) {
        File envDir = new File((String)dbProperties.get("database.url"));
        
        EnvironmentConfig ec = new EnvironmentConfig();
        ec.setAllowCreate(true);
        ec.setTransactional(true);
        ec.setDurability(Durability.COMMIT_NO_SYNC);
        //ec.setCachePercent(70);
        bdbJeEnv = new Environment(envDir, ec);
        dbMap = new ConcurrentHashMap<>();
        seqMap = new ConcurrentHashMap<>();
        bufAlloc = new PooledByteBufAllocator();
        objectPool = new GenericKeyedObjectPool<Class, ReusableMessageBuilder>(new CapnpStructBuilderFactory());
    }

    public Environment getBdbJeEnv() {
        return bdbJeEnv;
    }

    public Map<String, Database> getDbMap() {
        return dbMap;
    }

    public Map<Long, Sequence> getSeqMap() {
        return seqMap;
    }

    public PooledByteBufAllocator getBufAlloc() {
        return bufAlloc;
    }

    public GenericKeyedObjectPool<Class, ReusableMessageBuilder> getObjectPool() {
        return objectPool;
    }
}
