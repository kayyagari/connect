package com.mirth.connect.donkey.server.data.jdbc;

import java.sql.Connection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.mirth.connect.donkey.server.Donkey;
import com.mirth.connect.donkey.server.channel.Statistics;
import com.mirth.connect.donkey.server.controllers.ChannelController;
import com.mirth.connect.donkey.server.data.DonkeyDaoFactory;
import com.mirth.connect.donkey.server.data.StatisticsUpdater;
import com.mirth.connect.donkey.util.SerializerProvider;

public class BdbJeDaoFactory implements DonkeyDaoFactory {

    private Donkey donkey;
    private ChannelController channelController;
    private String statsServerId;
    private QuerySource querySource;
    private SerializerProvider serializerProvider;
    private StatisticsUpdater statisticsUpdater;
    private boolean encryptData = false;
    private boolean decryptData = true;
    private Map<Connection, PreparedStatementSource> statementSources = new ConcurrentHashMap<Connection, PreparedStatementSource>();
    private Logger logger = Logger.getLogger(getClass());

    public static BdbJeDaoFactory getInstance() {
        return new BdbJeDaoFactory();
    }

    protected BdbJeDaoFactory() {
        donkey = Donkey.getInstance();
        channelController = ChannelController.getInstance();
    }

    public String getStatsServerId() {
        return statsServerId;
    }

    public void setStatsServerId(String serverId) {
        statsServerId = serverId;
    }

    @Override
    public ConnectionPool getConnectionPool() {
        return null;
    }

    public void setConnectionPool(ConnectionPool connectionPool) {
    }

    public QuerySource getQuerySource() {
        return querySource;
    }

    public void setQuerySource(QuerySource querySource) {
        this.querySource = querySource;
    }

    public void setSerializerProvider(SerializerProvider serializerProvider) {
        this.serializerProvider = serializerProvider;
    }

    public Map<Connection, PreparedStatementSource> getStatementSources() {
        return statementSources;
    }

    @Override
    public void setEncryptData(boolean encryptData) {
        this.encryptData = encryptData;
    }

    @Override
    public void setDecryptData(boolean decryptData) {
        this.decryptData = decryptData;
    }

    @Override
    public void setStatisticsUpdater(StatisticsUpdater statisticsUpdater) {
        this.statisticsUpdater = statisticsUpdater;
    }

    @Override
    public BdbJeDao getDao() {
        return getDao(serializerProvider);
    }

    @Override
    public BdbJeDao getDao(SerializerProvider serializerProvider) {
        return getDao(donkey, null, querySource, null, serializerProvider, encryptData, decryptData, statisticsUpdater, channelController.getStatistics(), channelController.getTotalStatistics(), statsServerId);
    }

    protected BdbJeDao getDao(Donkey donkey, Connection connection, QuerySource querySource, PreparedStatementSource statementSource, SerializerProvider serializerProvider, boolean encryptData, boolean decryptData, StatisticsUpdater statisticsUpdater, Statistics currentStats, Statistics totalStats, String statsServerId) {
        return new BdbJeDao(donkey, connection, querySource, statementSource, serializerProvider, encryptData, decryptData, statisticsUpdater, currentStats, totalStats, statsServerId);
    }
}
