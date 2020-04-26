package com.mirth.connect.donkey.server.data.jdbc;

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
    private SerializerProvider serializerProvider;
    private StatisticsUpdater statisticsUpdater;
    private boolean encryptData = false;
    private boolean decryptData = true;
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

    public void setSerializerProvider(SerializerProvider serializerProvider) {
        this.serializerProvider = serializerProvider;
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
        return getDao(donkey, serializerProvider, encryptData, decryptData, statisticsUpdater, channelController.getStatistics(), channelController.getTotalStatistics(), statsServerId);
    }

    protected BdbJeDao getDao(Donkey donkey, SerializerProvider serializerProvider, boolean encryptData, boolean decryptData, StatisticsUpdater statisticsUpdater, Statistics currentStats, Statistics totalStats, String statsServerId) {
        return new BdbJeDao(donkey, serializerProvider, encryptData, decryptData, statisticsUpdater, currentStats, totalStats, statsServerId);
    }
}
