package com.mirth.connect.donkey.server.data.jdbc;

import static com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent.CapContentType.CHANNELMAP;
import static com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent.CapContentType.CONNECTORMAP;
import static com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent.CapContentType.ENCODED;
import static com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent.CapContentType.POSTPROCESSORERROR;
import static com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent.CapContentType.PROCESSEDRAW;
import static com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent.CapContentType.PROCESSEDRESPONSE;
import static com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent.CapContentType.PROCESSINGERROR;
import static com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent.CapContentType.RAW;
import static com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent.CapContentType.RESPONSE;
import static com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent.CapContentType.RESPONSEERROR;
import static com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent.CapContentType.RESPONSEMAP;
import static com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent.CapContentType.RESPONSETRANSFORMED;
import static com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent.CapContentType.SENT;
import static com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent.CapContentType.SOURCEMAP;
import static com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent.CapContentType.TRANSFORMED;
import static com.mirth.connect.donkey.util.SerializerUtil.bytesToInt;
import static com.mirth.connect.donkey.util.SerializerUtil.bytesToLong;
import static com.mirth.connect.donkey.util.SerializerUtil.intToBytes;
import static com.mirth.connect.donkey.util.SerializerUtil.longToBytes;
import static com.mirth.connect.donkey.util.SerializerUtil.readMessage;
import static com.mirth.connect.donkey.util.SerializerUtil.writeMessageToEntry;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.log4j.Logger;
import org.capnproto.MessageReader;
import org.capnproto.StructList;

import com.mirth.connect.donkey.model.channel.MetaDataColumn;
import com.mirth.connect.donkey.model.channel.MetaDataColumnType;
import com.mirth.connect.donkey.model.message.CapnpModel.CapAttachment;
import com.mirth.connect.donkey.model.message.CapnpModel.CapConnectorMessage;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMessage;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent.CapContentType;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMetadata;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMetadataColumn;
import com.mirth.connect.donkey.model.message.CapnpModel.CapStatistics;
import com.mirth.connect.donkey.model.message.ConnectorMessage;
import com.mirth.connect.donkey.model.message.ContentType;
import com.mirth.connect.donkey.model.message.ErrorContent;
import com.mirth.connect.donkey.model.message.MapContent;
import com.mirth.connect.donkey.model.message.Message;
import com.mirth.connect.donkey.model.message.MessageContent;
import com.mirth.connect.donkey.model.message.Status;
import com.mirth.connect.donkey.model.message.attachment.Attachment;
import com.mirth.connect.donkey.server.Donkey;
import com.mirth.connect.donkey.server.Encryptor;
import com.mirth.connect.donkey.server.channel.Channel;
import com.mirth.connect.donkey.server.channel.Statistics;
import com.mirth.connect.donkey.server.data.ChannelDoesNotExistException;
import com.mirth.connect.donkey.server.data.DonkeyDao;
import com.mirth.connect.donkey.server.data.DonkeyDaoException;
import com.mirth.connect.donkey.server.data.StatisticsUpdater;
import com.mirth.connect.donkey.util.MapUtil;
import com.mirth.connect.donkey.util.SerializerProvider;
import com.mirth.connect.donkey.util.SerializerUtil;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.SequenceConfig;
import com.sleepycat.je.Transaction;

import io.netty.buffer.PooledByteBufAllocator;

public class BdbJeDao implements DonkeyDao {
    private Donkey donkey;
    private SerializerProvider serializerProvider;
    private boolean encryptData;
    private boolean decryptData;
    private StatisticsUpdater statisticsUpdater;
    private Set<ContentType> alwaysDecrypt = new HashSet<ContentType>();
    private Encryptor encryptor;
    private Statistics currentStats;
    private Statistics totalStats;
    private Statistics transactionStats = new Statistics(false, true);
    private Map<String, Map<Integer, Set<Status>>> resetCurrentStats = new HashMap<String, Map<Integer, Set<Status>>>();
    private Map<String, Map<Integer, Set<Status>>> resetTotalStats = new HashMap<String, Map<Integer, Set<Status>>>();
    private List<String> removedChannelIds = new ArrayList<String>();
    private String asyncCommitCommand;
    private Map<String, Long> localChannelIds;
    private String statsServerId;
    private boolean transactionAlteredChannels = false;
    private char quoteChar = '"';
    private Logger logger = Logger.getLogger(this.getClass());

    private Environment bdbJeEnv;

    private Transaction txn;
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    public static final String TABLE_D_CHANNELS = "d_channels";
    public static final String TABLE_D_MESSAGE_SEQ = "d_msq";
    public static final String TABLE_D_META_COLUMNS = "d_mcm_columns";

    private Map<String, Database> dbMap;
    private Map<Long, Sequence> seqMap;
    private PooledByteBufAllocator bufAlloc;
    private GenericKeyedObjectPool<Class, ReusableMessageBuilder> objectPool;

    private static final SequenceConfig SEQ_CONF = new SequenceConfig();
    private static final byte DELIM_BYTE = '\\';

    static {
        SEQ_CONF.setAllowCreate(true);
        SEQ_CONF.setInitialValue(1);
        SEQ_CONF.setCacheSize(1000);
    }

    protected BdbJeDao(Donkey donkey, SerializerProvider serializerProvider, boolean encryptData, boolean decryptData, StatisticsUpdater statisticsUpdater, Statistics currentStats, Statistics totalStats, String statsServerId) {
        this.donkey = donkey;
        bdbJeEnv = donkey.getBdbJeEnv();
        dbMap = donkey.getDbMap();
        seqMap = donkey.getSeqMap();
        bufAlloc = donkey.getBufAlloc();
        objectPool = donkey.getObjectPool();
        this.serializerProvider = serializerProvider;
        this.encryptData = encryptData;
        this.decryptData = decryptData;
        this.statisticsUpdater = statisticsUpdater;
        this.currentStats = currentStats;
        this.totalStats = totalStats;
        this.statsServerId = statsServerId;
        encryptor = donkey.getEncryptor();
        alwaysDecrypt.addAll(Arrays.asList(ContentType.getMapTypes()));
        alwaysDecrypt.addAll(Arrays.asList(ContentType.getErrorTypes()));

        txn = bdbJeEnv.beginTransaction(null, null);
        logger.debug("Opened connection");
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

    public char getQuoteChar() {
        return quoteChar;
    }

    public void setQuoteChar(char quoteChar) {
        this.quoteChar = quoteChar;
    }

    @Override
    public void insertMessage(Message message) {
        logger.debug(message.getChannelId() + "/" + message.getMessageId() + ": inserting message");

        ReusableMessageBuilder rmb = null;

        try {
            rmb = objectPool.borrowObject(CapMessage.class);
            CapMessage.Builder mb = (CapMessage.Builder)rmb.getSb();
            mb.setChannelId(message.getChannelId());
            if(message.getImportChannelId() != null) {
                mb.setImportChannelId(message.getImportChannelId());
            }
            if(message.getImportId() == null) {
                mb.setImportId(-1);
            }
            else {
                mb.setImportId(message.getImportId());
            }
            mb.setMessageId(message.getMessageId());
            long originalId = -1;
            if(message.getOriginalId() != null) {
                originalId = message.getOriginalId();
            }
            mb.setOriginalId(originalId);

            mb.setProcessed(message.isProcessed());
            mb.setReceivedDate(message.getReceivedDate().getTimeInMillis());
            mb.setServerId(message.getServerId());
            
            long localChannelId = getLocalChannelId(message.getChannelId());
            DatabaseEntry key = new DatabaseEntry(longToBytes(message.getMessageId()));
            DatabaseEntry data = new DatabaseEntry();
            writeMessageToEntry(rmb.getMb(), bufAlloc.buffer(), data);
            dbMap.get("d_m" + localChannelId).put(txn, key, data);
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        } finally {
            if(rmb != null) {
                objectPool.returnObject(CapMessage.class, rmb);
            }
        }
    }

    @Override
    public void updateSendAttempts(ConnectorMessage connectorMessage) {
        logger.debug(connectorMessage.getChannelId() + "/" + connectorMessage.getMessageId() + ": updating send attempts");

        Calendar sendDate = connectorMessage.getSendDate();
        Calendar responseDate = connectorMessage.getResponseDate();

        ReusableMessageBuilder rmb = null;
        try {
            DatabaseEntry key = new DatabaseEntry(buildPrimaryKeyOfConnectorMessage(connectorMessage.getMessageId(), connectorMessage.getMetaDataId()));
            long cid = getLocalChannelId(connectorMessage.getChannelId());
            DatabaseEntry data = new DatabaseEntry();
            Database cmDb = dbMap.get("d_mm" + cid);
            OperationStatus os = cmDb.get(txn, key, data, LockMode.READ_COMMITTED);
            if(os == OperationStatus.SUCCESS) {
                MessageReader mr = readMessage(data.getData());
                CapConnectorMessage.Reader atReader= mr.getRoot(CapConnectorMessage.factory);

                rmb = objectPool.borrowObject(CapConnectorMessage.class);
                rmb.getMb().setRoot(CapConnectorMessage.factory, atReader);
                CapConnectorMessage.Builder cb = (CapConnectorMessage.Builder)rmb.getSb();
                cb.setSendAttempts(connectorMessage.getSendAttempts());
                
                if(sendDate != null) {
                    cb.setSendDate(sendDate.getTimeInMillis());
                }
                
                if(responseDate != null) {
                    cb.setResponseDate(responseDate.getTimeInMillis());
                }
                
                writeMessageToEntry(rmb.getMb(), bufAlloc.buffer(), data);
                cmDb.put(txn, key, data);
            }
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        } finally {
            if(rmb != null) {
                objectPool.returnObject(CapConnectorMessage.class, rmb);
            }
        }
    }

    @Override
    public void insertMessageContent(MessageContent messageContent) {
        upsertMessageContent(messageContent.getChannelId(), messageContent.getMessageId(), messageContent.getMetaDataId(), messageContent.getContentType(), messageContent.getContent(), messageContent.getDataType(), messageContent.isEncrypted(), false);
    }
    
//    private void upsertMessageContent(MessageContent messageContent, boolean update) {
    private void upsertMessageContent(String channelId, long messageId, int metaDataId, ContentType contentType, String content, String dataType, boolean encrypted, boolean update) {
        String inserting = update ? "updating" : "inserting";
        String logMsg = String.format("%s/%d/%d: %s message content (%s)", channelId, messageId, metaDataId, inserting, contentType.toString());
        logger.debug(logMsg);

        //insertContent(messageContent.getChannelId(), messageContent.getMessageId(), messageContent.getMetaDataId(), messageContent.getContentType(), messageContent.getContent(), messageContent.getDataType(), messageContent.isEncrypted());
        ReusableMessageBuilder rmb = null;
        try {
            rmb = objectPool.borrowObject(CapMessageContent.class);
            CapMessageContent.Builder cb = (CapMessageContent.Builder) rmb.getSb();

            // Only encrypt if the content is not already encrypted
            if (encryptData && encryptor != null && !encrypted) {
                content = encryptor.encrypt(content);
                encrypted = true;
            }

            cb.setContent(content);
            cb.setContentType(toCapContentType(contentType));
            if(dataType != null) {
                cb.setDataType(dataType);
            }
            cb.setEncrypted(encrypted);
            cb.setMessageId(messageId);
            cb.setMetaDataId(metaDataId);

            long localChannelId = getLocalChannelId(channelId);
            DatabaseEntry key = new DatabaseEntry(buildPrimaryKeyOfMessageContent(messageId, metaDataId, contentType));
            DatabaseEntry data = new DatabaseEntry();
            writeMessageToEntry(rmb.getMb(), bufAlloc.buffer(), data);
            Database msgContentDb = dbMap.get("d_mc" + localChannelId);
            msgContentDb.put(txn, key, data);
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        } finally {
            if(rmb != null) {
                objectPool.returnObject(CapMessageContent.class, rmb);
            }
        }
    }

    @Override
    public void batchInsertMessageContent(MessageContent messageContent) {
        insertMessageContent(messageContent);
    }

    @Override
    public void executeBatchInsertMessageContent(String channelId) {}

    @Override
    public void storeMessageContent(MessageContent messageContent) {
        logger.debug(messageContent.getChannelId() + "/" + messageContent.getMessageId() + "/" + messageContent.getMetaDataId() + ": updating message content (" + messageContent.getContentType().toString() + ")");

        //storeContent(messageContent.getChannelId(), messageContent.getMessageId(), messageContent.getMetaDataId(), messageContent.getContentType(), messageContent.getContent(), messageContent.getDataType(), messageContent.isEncrypted());
        upsertMessageContent(messageContent.getChannelId(), messageContent.getMessageId(), messageContent.getMetaDataId(), messageContent.getContentType(), messageContent.getContent(), messageContent.getDataType(), messageContent.isEncrypted(), true);
    }

    public void storeContent(String channelId, long messageId, int metaDataId, ContentType contentType, String content, String dataType, boolean encrypted) {
        upsertMessageContent(channelId, messageId, metaDataId, contentType, content, dataType, encrypted, true);
    }

    private void insertContent(String channelId, long messageId, int metaDataId, ContentType contentType, String content, String dataType, boolean encrypted) {
        upsertMessageContent(channelId, messageId, metaDataId, contentType, content, dataType, encrypted, false);
    }

    @Override
    public void addChannelStatistics(Statistics statistics) {
        Set<String> failedChannelIds = null;

        for (Entry<String, Map<Integer, Map<Status, Long>>> channelEntry : statistics.getStats().entrySet()) {
            try {
                String channelId = channelEntry.getKey();
                Map<Integer, Map<Status, Long>> channelAndConnectorStats = channelEntry.getValue();
                Map<Integer, Map<Status, Long>> connectorStatsToUpdate = new HashMap<Integer, Map<Status, Long>>();
                Map<Status, Long> channelStats = channelAndConnectorStats.get(null);

                for (Entry<Integer, Map<Status, Long>> entry : channelAndConnectorStats.entrySet()) {
                    Integer metaDataId = entry.getKey();

                    // only add connector stats to the statsToUpdate list, not the channel stats
                    if (metaDataId != null) {
                        Map<Status, Long> connectorStats = entry.getValue();

                        if (hasUpdatableStatistics(connectorStats)) {
                            connectorStatsToUpdate.put(metaDataId, connectorStats);
                        }
                    }
                }

                /*
                 * MIRTH-3042: With certain channel configurations, SQL Server will encounter a
                 * deadlock scenario unless we always update the channel stats row and update it
                 * before the connector stats. We determined that this is because SQL Server creates
                 * a page lock when the statistics update statement references the existing row
                 * values in order to increment them (RECEIVED = RECEIVED + ?). Other databases such
                 * as Postgres use only row locks in this situation so they were not deadlocking.
                 * The deadlock scenario was only confirmed to happen with a channel with multiple
                 * asynchronous destinations and destination queues enabled.
                 */
                if (!connectorStatsToUpdate.isEmpty() || hasUpdatableStatistics(channelStats)) {
                    updateStatistics(channelId, null, channelStats);

                    for (Entry<Integer, Map<Status, Long>> entry : connectorStatsToUpdate.entrySet()) {
                        updateStatistics(channelId, entry.getKey(), entry.getValue());
                    }
                }
            } catch (ChannelDoesNotExistException e) {
                if (failedChannelIds == null) {
                    failedChannelIds = new HashSet<String>();
                }
                failedChannelIds.addAll(e.getChannelIds());
            }
        }

        if (failedChannelIds != null) {
            throw new ChannelDoesNotExistException(failedChannelIds);
        }
    }

    private boolean hasUpdatableStatistics(Map<Status, Long> stats) {
        return (stats.get(Status.RECEIVED) != 0 || stats.get(Status.FILTERED) != 0 || stats.get(Status.SENT) != 0 || stats.get(Status.ERROR) != 0);
    }

    private void updateStatistics(String channelId, Integer metaDataId, Map<Status, Long> stats) {
        long received = stats.get(Status.RECEIVED);
        long filtered = stats.get(Status.FILTERED);
        long sent = stats.get(Status.SENT);
        long error = stats.get(Status.ERROR);

        logger.debug(channelId + "/" + metaDataId + ": saving statistics");

        ReusableMessageBuilder rmb = null;
        try {
            if (metaDataId == null) {
                metaDataId = -1; // indicates this row contains channel stats
            }

            DatabaseEntry key = new DatabaseEntry(buildPrimaryKeyOfStats(metaDataId));
            long cid = getLocalChannelId(channelId);
            Database statsDb = dbMap.get("d_ms" + cid);
            DatabaseEntry data = new DatabaseEntry();
            OperationStatus os = statsDb.get(txn, key, data, LockMode.READ_COMMITTED);

            rmb = objectPool.borrowObject(CapStatistics.class);
            CapStatistics.Builder cb = null;
            if(os == OperationStatus.SUCCESS) {
                MessageReader mr = readMessage(data.getData());
                CapStatistics.Reader atReader= mr.getRoot(CapStatistics.factory);
                rmb.getMb().setRoot(CapStatistics.factory, atReader);
                cb = (CapStatistics.Builder)rmb.getSb();
            }
            else {
                cb = (CapStatistics.Builder)rmb.getSb();
            }

            long tmpReceived = cb.getReceived() + received;
            if(tmpReceived < 0) {
                tmpReceived = 0;
            }
            cb.setReceived(tmpReceived);
            
            long tmpReceivedLife = cb.getReceivedLifetime() + received;
            if(tmpReceivedLife < 0) {
                tmpReceivedLife = 0;
            }
            cb.setReceivedLifetime(tmpReceivedLife);

            long tmpFiltered = cb.getFiltered() + filtered;
            if(tmpFiltered < 0) {
                tmpFiltered = 0;
            }
            cb.setFiltered(tmpFiltered);

            long tmpFilteredLifetime = cb.getFilteredLifetime() + filtered;
            if(tmpFilteredLifetime < 0) {
                tmpFilteredLifetime = 0;
            }
            cb.setFilteredLifetime(tmpFilteredLifetime);
            
            long tmpSent = cb.getSent() + sent;
            if(tmpSent < 0) {
                tmpSent = 0;
            }
            cb.setSent(tmpSent);

            long tmpSentLifetime = cb.getSentLifetime() + sent;
            if(tmpSentLifetime < 0) {
                tmpSentLifetime = 0;
            }
            cb.setSentLifetime(tmpSentLifetime);
            
            long tmpError = cb.getError() + error;
            if(tmpError < 0) {
                tmpError = 0;
            }
            cb.setError(tmpError);

            long tmpErrorLifetime = cb.getError() + error;
            if(tmpErrorLifetime < 0) {
                tmpErrorLifetime = 0;
            }
            cb.setErrorLifetime(tmpErrorLifetime);
            
            writeMessageToEntry(rmb.getMb(), bufAlloc.buffer(), data);
            statsDb.put(txn, key, data);
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        } finally {
            if(rmb != null) {
                objectPool.returnObject(CapStatistics.class, rmb);
            }
        }
    }

    @Override
    public void insertMessageAttachment(String channelId, long messageId, Attachment attachment) {
        logger.debug(channelId + "/" + messageId + ": inserting message attachment");
        upsertMessageAttachment(channelId, messageId, attachment);
    }
    
    private void upsertMessageAttachment(String channelId, long messageId, Attachment attachment) {
        ReusableMessageBuilder rmb = null;
        try {
            rmb = objectPool.borrowObject(CapAttachment.class);
            CapAttachment.Builder cab = (CapAttachment.Builder)rmb.getSb();
            cab.setContent(attachment.getContent());
            cab.setId(attachment.getId());
            cab.setType(attachment.getType());
            cab.setAttachmentSize(attachment.getContent().length);
            cab.setMessageId(messageId);
            
            DatabaseEntry key = new DatabaseEntry(buildPrimaryKeyOfAttachment(messageId, attachment.getAttachmentId()));
            DatabaseEntry data = new DatabaseEntry();
            writeMessageToEntry(rmb.getMb(), bufAlloc.buffer(), data);
            long localChannelId = getLocalChannelId(channelId);
            dbMap.get("d_ma" + localChannelId).put(txn, key, data);
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        } finally {
            if(rmb != null) {
                objectPool.returnObject(CapAttachment.class, rmb);
            }
        }
    }

    @Override
    public void updateMessageAttachment(String channelId, long messageId, Attachment attachment) {
        logger.debug(channelId + "/" + messageId + ": updating message attachment");
        upsertMessageAttachment(channelId, messageId, attachment);
    }

    @Override
    public void insertMetaData(ConnectorMessage connectorMessage, List<MetaDataColumn> metaDataColumns) {
        logger.debug(connectorMessage.getChannelId() + "/" + connectorMessage.getMessageId() + "/" + connectorMessage.getMetaDataId() + ": inserting custom meta data");
        upsertMetaData(connectorMessage, metaDataColumns);
    }

    private void upsertMetaData(ConnectorMessage connectorMessage, List<MetaDataColumn> metaDataColumns) {
        ReusableMessageBuilder rmb = null;
        try {
            List<String> metaDataColumnNames = new ArrayList<String>();
            Map<String, Object> metaDataMap = connectorMessage.getMetaDataMap();

            for (MetaDataColumn metaDataColumn : metaDataColumns) {
                Object value = metaDataMap.get(metaDataColumn.getName());

                if (value != null) {
                    metaDataColumnNames.add(metaDataColumn.getName());
                }
            }

            // Don't do anything if all values were null
            if (!metaDataColumnNames.isEmpty()) {
                rmb = objectPool.borrowObject(CapMetadata.class);
                CapMetadata.Builder cb = (CapMetadata.Builder)rmb.getSb();
                StructList.Builder<CapMetadataColumn.Builder> colLstBuilder = cb.initColumns(metaDataColumnNames.size());

                cb.setMetadataId(connectorMessage.getMetaDataId());
                cb.setMessageId(connectorMessage.getMessageId());
                int n = 0;

                for (MetaDataColumn metaDataColumn : metaDataColumns) {
                    CapMetadataColumn.Builder column = colLstBuilder.get(n);
                    Object value = metaDataMap.get(metaDataColumn.getName());
                    if(value == null) {
                        continue;
                    }

                    column.setName(metaDataColumn.getName());
                    // @formatter:off
                    switch (metaDataColumn.getType()) {
                    case STRING:
                        column.setType(CapMetadataColumn.Type.STRING);
                        column.setValue((String) value);
                        break;
                    case NUMBER:
                        column.setType(CapMetadataColumn.Type.NUMBER);
                        column.setValue(((BigDecimal)value).toString());
                        break;
                    case BOOLEAN:
                        column.setType(CapMetadataColumn.Type.BOOLEAN);
                        column.setValue(value.toString());
                        break;
                    case TIMESTAMP:
                        column.setType(CapMetadataColumn.Type.TIMESTAMP);
                        column.setValue(String.valueOf(((Calendar) value).getTimeInMillis()));
                        break;
                    }
                    // @formatter:on
                    
                    n++;
                }
                
                long cid = getLocalChannelId(connectorMessage.getChannelId());
                DatabaseEntry key = new DatabaseEntry(buildPrimaryKeyOfMetadata(connectorMessage.getMessageId(), connectorMessage.getMetaDataId()));
                DatabaseEntry data = new DatabaseEntry();
                writeMessageToEntry(rmb.getMb(), bufAlloc.buffer(), data);
                dbMap.get("d_mcm" + cid).put(txn, key, data);
            }
        } catch (Exception e) {
            throw new DonkeyDaoException("Failed to insert connector message meta data", e);
        } finally {
            if(rmb != null) {
                objectPool.returnObject(CapMetadata.class, rmb);
            }
        }
    }

    @Override
    public void storeMetaData(ConnectorMessage connectorMessage, List<MetaDataColumn> metaDataColumns) {
        logger.debug(connectorMessage.getChannelId() + "/" + connectorMessage.getMessageId() + "/" + connectorMessage.getMetaDataId() + ": updating custom meta data");
        upsertMetaData(connectorMessage, metaDataColumns);
    }

    @Override
    public void insertConnectorMessage(ConnectorMessage connectorMessage, boolean storeMaps, boolean updateStats) {
        logger.debug(connectorMessage.getChannelId() + "/" + connectorMessage.getMessageId() + "/" + connectorMessage.getMetaDataId() + ": inserting connector message with" + (storeMaps ? "" : "out") + " maps");

        ReusableMessageBuilder rmb = null;
        try {
            _updateErrorsWithoutConnectorMessageUpdate(connectorMessage);
            rmb = objectPool.borrowObject(CapConnectorMessage.class);
            CapConnectorMessage.Builder cb = (CapConnectorMessage.Builder)rmb.getSb();
            cb.setId(connectorMessage.getMetaDataId());
            cb.setMessageId(connectorMessage.getMessageId());
            cb.setServerId(connectorMessage.getServerId());
            cb.setReceivedDate(connectorMessage.getReceivedDate().getTimeInMillis());
            
            cb.setConnectorName(connectorMessage.getConnectorName());
            cb.setSendAttempts(connectorMessage.getSendAttempts());
            Calendar sendDate = connectorMessage.getSendDate();
            if(sendDate != null) {
                cb.setSendDate(sendDate.getTimeInMillis());
            }

            Calendar respDate = connectorMessage.getResponseDate();
            if(respDate != null) {
                cb.setResponseDate(respDate.getTimeInMillis());
            }
            cb.setErrorCode(connectorMessage.getErrorCode());
            cb.setChainId(connectorMessage.getChainId());
            cb.setOrderId(connectorMessage.getOrderId());

            long localchannelId = getLocalChannelId(connectorMessage.getChannelId());
            DatabaseEntry key = new DatabaseEntry(buildPrimaryKeyOfConnectorMessage(connectorMessage.getMessageId(), connectorMessage.getMetaDataId()));
            DatabaseEntry data = new DatabaseEntry();
            writeMessageToEntry(rmb.getMb(), bufAlloc.buffer(), data);
            
            dbMap.get("d_mm" + localchannelId).put(txn, key, data);
            Database cmStatusDb = dbMap.get("d_mm_status" + localchannelId);
            key.setData(buildPrimaryKeyOfConnectorMessage(connectorMessage.getMessageId(), connectorMessage.getMetaDataId()));
            cmStatusDb.put(txn, key, new DatabaseEntry(connectorMessage.getStatus().getData()));

            if (storeMaps) {
                updateSourceMap(connectorMessage);
                updateMaps(connectorMessage);
            }

            //updateErrors(connectorMessage);

            if (updateStats) {
                transactionStats.update(connectorMessage.getChannelId(), connectorMessage.getMetaDataId(), connectorMessage.getStatus(), null);
            }
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        } finally {
            if(rmb != null) {
                objectPool.returnObject(CapConnectorMessage.class, rmb);
            }
        }
    }

    @Override
    public void deleteMessage(String channelId, long messageId) {
        logger.debug(channelId + "/" + messageId + ": deleting message");

        try {
            long cid = getLocalChannelId(channelId);
            DatabaseEntry key = new DatabaseEntry(longToBytes(messageId));
            //cascadeMessageDelete("deleteMessageCascadeAttachments", messageId, channelId);
            dbMap.get("d_ma" + cid).delete(txn, key);

            //cascadeMessageDelete("deleteMessageCascadeMetadata", messageId, channelId);
            Database metadataDb = dbMap.get("d_mcm" + cid);
            deleteAllStartingwith(messageId, metadataDb);
            
            //cascadeMessageDelete("deleteMessageCascadeContent", messageId, channelId);
            Database msgContentDb = dbMap.get("d_mc" + cid);
            deleteAllStartingwith(messageId, msgContentDb);

            //cascadeMessageDelete("deleteMessageCascadeConnectorMessage", messageId, channelId);
            Database cmDb = dbMap.get("d_mm" + cid);
            deleteAllStartingwith(messageId, cmDb);
            
            dbMap.get("d_m" + cid).delete(txn, key);
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        }
    }

    @Override
    public void deleteConnectorMessages(String channelId, long messageId, Set<Integer> metaDataIds) {
        logger.debug(channelId + "/" + messageId + ": deleting connector messages");
        long cid = getLocalChannelId(channelId);

        try {
            if (metaDataIds == null) {
                //cascadeMessageDelete("deleteMessageCascadeMetadata", messageId, channelId);
                Database metadataDb = dbMap.get("d_mcm" + cid);
                deleteAllStartingwith(messageId, metadataDb);

                //cascadeMessageDelete("deleteMessageCascadeContent", messageId, channelId);
                Database msgContentDb = dbMap.get("d_mc" + cid);
                deleteAllStartingwith(messageId, msgContentDb);

                //statement = prepareStatement("deleteConnectorMessages", channelId);
                Database cmDb = dbMap.get("d_mm" + cid);
                deleteAllStartingwith(messageId, cmDb);
            } else {
                Map<String, Object> values = new HashMap<String, Object>();
                values.put("localChannelId", cid);
                values.put("metaDataIds", StringUtils.join(metaDataIds, ','));

                Database msgContentDb = dbMap.get("d_mc" + cid);
                Database metadataDb = dbMap.get("d_mcm" + cid);
                Database mcDb = dbMap.get("d_mm" + cid);
                byte[] keyData = buildPrimaryKeyOfConnectorMessage(messageId, 0);
                DatabaseEntry key = new DatabaseEntry(keyData);
                for(int mid : metaDataIds) {
                    intToBytes(mid, keyData, 8); // the same key is useful for all the below DBs
                    msgContentDb.delete(txn, key);
                    metadataDb.delete(txn, key);
                    mcDb.delete(txn, key);
                }
            }
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        }
    }

    @Override
    public void deleteMessageStatistics(String channelId, long messageId, Set<Integer> metaDataIds) {
        Map<Integer, ConnectorMessage> connectorMessages = getConnectorMessages(channelId, messageId, new ArrayList<Integer>(metaDataIds));
        ConnectorMessage sourceMessage = connectorMessages.get(0);

        /*
         * The server id on the source message indicates which server last processed/reprocessed the
         * message. We only want to delete the statistics if the current stats server
         * (statsServerId) is the server that last processed or reprocessed the message.
         */
        if (sourceMessage != null && sourceMessage.getServerId().equals(statsServerId)) {
            for (Entry<Integer, ConnectorMessage> entry : connectorMessages.entrySet()) {
                Integer metaDataId = entry.getKey();
                ConnectorMessage connectorMessage = entry.getValue();

                /*
                 * We also test if each connector message belongs to the statsServerId before
                 * deleting.
                 */
                if (connectorMessage.getServerId().equals(statsServerId) && (metaDataIds == null || metaDataIds.contains(metaDataId))) {
                    Status status = connectorMessage.getStatus();

                    Map<Status, Long> statsDiff = new HashMap<Status, Long>();
                    statsDiff.put(Status.RECEIVED, -1L);
                    statsDiff.put(status, -1L);

                    transactionStats.update(channelId, metaDataId, statsDiff);
                }
            }
        }
    }

    @Override
    public void updateStatus(ConnectorMessage connectorMessage, Status previousStatus) {
        logger.debug(connectorMessage.getChannelId() + "/" + connectorMessage.getMessageId() + "/" + connectorMessage.getMetaDataId() + ": updating status from " + previousStatus.getStatusCode() + " to " + connectorMessage.getStatus().getStatusCode());

        // don't decrement the previous status if it was RECEIVED
        if (previousStatus == Status.RECEIVED) {
            previousStatus = null;
        }
        
        String channelId = connectorMessage.getChannelId();
        transactionStats.update(channelId, connectorMessage.getMetaDataId(), connectorMessage.getStatus(), previousStatus);
        
        long cid = getLocalChannelId(channelId);
        Database cmStatusDb = dbMap.get("d_mm_status" + cid);
        DatabaseEntry key = new DatabaseEntry(buildPrimaryKeyOfConnectorMessage(connectorMessage.getMessageId(), connectorMessage.getMetaDataId()));
        OperationStatus os = cmStatusDb.put(txn, key, new DatabaseEntry(connectorMessage.getStatus().getData()));
        if(os != OperationStatus.SUCCESS) {
            throw new DonkeyDaoException("Failed to update connector message status, the connector message was removed from this server.");
        }
    }

    private boolean updateConnectorMessage(ConnectorMessage connectorMessage, boolean error) {
        ReusableMessageBuilder rmb = null;
        try {
            DatabaseEntry key = new DatabaseEntry(buildPrimaryKeyOfConnectorMessage(connectorMessage.getMessageId(), connectorMessage.getMetaDataId()));
            long cid = getLocalChannelId(connectorMessage.getChannelId());
            DatabaseEntry data = new DatabaseEntry();
            Database cmDb = dbMap.get("d_mm" + cid);
            OperationStatus os = cmDb.get(txn, key, data, LockMode.READ_COMMITTED);
            if(os == OperationStatus.SUCCESS) {
                MessageReader mr = readMessage(data.getData());
                CapConnectorMessage.Reader atReader= mr.getRoot(CapConnectorMessage.factory);
                //System.out.println(atReader.getId() + " " + atReader.getMessageId());

                rmb = objectPool.borrowObject(CapConnectorMessage.class);
                rmb.getMb().setRoot(CapConnectorMessage.factory, atReader);
                CapConnectorMessage.Builder cb = (CapConnectorMessage.Builder)rmb.getSb();

                if(error) {
                    cb.setErrorCode(connectorMessage.getErrorCode());
                }

                writeMessageToEntry(rmb.getMb(), bufAlloc.buffer(), data);
                cmDb.put(txn, key, data);
                return true;
            }
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        } finally {
            if(rmb != null) {
                objectPool.returnObject(CapConnectorMessage.class, rmb);
            }
        }
        return false;
    }
    
    @Override
    public void updateErrors(ConnectorMessage connectorMessage) {
        boolean errorsUpdated = _updateErrorsWithoutConnectorMessageUpdate(connectorMessage);
        if (errorsUpdated) {
            updateErrorCode(connectorMessage);
        }
    }
    
    private boolean _updateErrorsWithoutConnectorMessageUpdate(ConnectorMessage connectorMessage) {
        logger.debug(connectorMessage.getChannelId() + "/" + connectorMessage.getMessageId() + "/" + connectorMessage.getMetaDataId() + ": updating errors");

        boolean errorsUpdated = false;

        if (updateError(connectorMessage.getProcessingErrorContent(), connectorMessage.getChannelId(), connectorMessage.getMessageId(), connectorMessage.getMetaDataId(), ContentType.PROCESSING_ERROR)) {
            errorsUpdated = true;
        }

        if (updateError(connectorMessage.getPostProcessorErrorContent(), connectorMessage.getChannelId(), connectorMessage.getMessageId(), connectorMessage.getMetaDataId(), ContentType.POSTPROCESSOR_ERROR)) {
            errorsUpdated = true;
        }

        if (updateError(connectorMessage.getResponseErrorContent(), connectorMessage.getChannelId(), connectorMessage.getMessageId(), connectorMessage.getMetaDataId(), ContentType.RESPONSE_ERROR)) {
            errorsUpdated = true;
        }
        
        return errorsUpdated;
    }

    private boolean updateError(ErrorContent errorContent, String channelId, long messageId, int metaDataId, ContentType contentType) {
        String error = errorContent.getContent();
        boolean encrypted = errorContent.isEncrypted();
        boolean persisted = errorContent.isPersisted();

        if (StringUtils.isNotEmpty(error)) {
            if (persisted) {
                storeContent(channelId, messageId, metaDataId, contentType, error, null, encrypted);
            } else {
                insertContent(channelId, messageId, metaDataId, contentType, error, null, encrypted);
                errorContent.setPersisted(true);
            }
        } else if (persisted) {
            deleteMessageContentByMetaDataIdAndContentType(channelId, messageId, metaDataId, contentType);
        } else {
            return false;
        }

        return true;
    }

    private void updateErrorCode(ConnectorMessage connectorMessage) {
        updateConnectorMessage(connectorMessage, true);
    }

    @Override
    public void updateMaps(ConnectorMessage connectorMessage) {
        logger.debug(connectorMessage.getChannelId() + "/" + connectorMessage.getMessageId() + "/" + connectorMessage.getMetaDataId() + ": updating maps");

        // We do not include the source map here because that should only be inserted once with the raw content, and after that it's read-only
        updateMap(connectorMessage.getConnectorMapContent(), connectorMessage.getChannelId(), connectorMessage.getMessageId(), connectorMessage.getMetaDataId(), ContentType.CONNECTOR_MAP);
        updateMap(connectorMessage.getChannelMapContent(), connectorMessage.getChannelId(), connectorMessage.getMessageId(), connectorMessage.getMetaDataId(), ContentType.CHANNEL_MAP);
        updateMap(connectorMessage.getResponseMapContent(), connectorMessage.getChannelId(), connectorMessage.getMessageId(), connectorMessage.getMetaDataId(), ContentType.RESPONSE_MAP);
    }

    private void updateMap(MapContent mapContent, String channelId, long messageId, int metaDataId, ContentType contentType) {
        if (mapContent != null) {
            boolean encrypted = mapContent.isEncrypted();
            boolean persisted = mapContent.isPersisted();

            String content = null;
            if (encrypted) {
                content = (String) mapContent.getContent();
            } else {
                Map<String, Object> map = mapContent.getMap();
                if (MapUtils.isNotEmpty(map)) {
                    content = MapUtil.serializeMap(serializerProvider.getSerializer(metaDataId), map);
                }
            }

            if (content != null) {
                if (persisted) {
                    storeContent(channelId, messageId, metaDataId, contentType, content, null, encrypted);
                } else {
                    insertContent(channelId, messageId, metaDataId, contentType, content, null, encrypted);
                    mapContent.setPersisted(true);
                }
            } else if (persisted) {
                deleteMessageContentByMetaDataIdAndContentType(channelId, messageId, metaDataId, contentType);
            }
        }
    }

    @Override
    public void updateSourceMap(ConnectorMessage connectorMessage) {
        // Only insert the source map content for the source connector message
        if (connectorMessage.getMetaDataId() == 0) {
            logger.debug(connectorMessage.getChannelId() + "/" + connectorMessage.getMessageId() + "/" + connectorMessage.getMetaDataId() + ": updating source map");

            updateMap(connectorMessage.getSourceMapContent(), connectorMessage.getChannelId(), connectorMessage.getMessageId(), connectorMessage.getMetaDataId(), ContentType.SOURCE_MAP);
        }
    }

    @Override
    public void updateResponseMap(ConnectorMessage connectorMessage) {
        logger.debug(connectorMessage.getChannelId() + "/" + connectorMessage.getMessageId() + "/" + connectorMessage.getMetaDataId() + ": updating response map");

        updateMap(connectorMessage.getResponseMapContent(), connectorMessage.getChannelId(), connectorMessage.getMessageId(), connectorMessage.getMetaDataId(), ContentType.RESPONSE_MAP);
    }

    @Override
    public void markAsProcessed(String channelId, long messageId) {
        logger.debug(channelId + "/" + messageId + ": marking as processed");
        markAsProcessedOrReset(channelId, messageId, false);
    }

    private void markAsProcessedOrReset(String channelId, long messageId, boolean reset) {
        ReusableMessageBuilder rmb = null;
        try {
            long localChannelId = getLocalChannelId(channelId);
            DatabaseEntry key = new DatabaseEntry(longToBytes(messageId));
            DatabaseEntry data = new DatabaseEntry();
            Database messageDb = dbMap.get("d_m" + localChannelId);
            OperationStatus os = messageDb.get(txn, key, data, LockMode.READ_COMMITTED);
            if(os == OperationStatus.SUCCESS) {
                MessageReader mr = readMessage(data.getData());
                CapMessage.Reader atReader= mr.getRoot(CapMessage.factory);

                rmb = objectPool.borrowObject(CapMessage.class);
                rmb.getMb().setRoot(CapMessage.factory, atReader);
                CapMessage.Builder mb = (CapMessage.Builder)rmb.getSb();
                mb.setProcessed(true);

                if(reset) {
                    mb.setImportChannelId((String)null);
                    mb.setImportId(-1);
                }

                writeMessageToEntry(rmb.getMb(), bufAlloc.buffer(), data);
                messageDb.put(txn, key, data);
            }
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        }
        finally {
            if(rmb != null) {
                objectPool.returnObject(CapMessage.class, rmb);
            }
        }
    }

    @Override
    public void resetMessage(String channelId, long messageId) {
        logger.debug(channelId + "/" + messageId + ": resetting message");
        markAsProcessedOrReset(channelId, messageId, true);
    }

    @Override
    public Map<String, Long> getLocalChannelIds() {
        if (localChannelIds == null) {
            CursorConfig cc = new CursorConfig();
            cc.setReadCommitted(true);
            Cursor cursor = dbMap.get(TABLE_D_CHANNELS).openCursor(txn, cc);
            try {
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry val = new DatabaseEntry();
                localChannelIds = new HashMap<String, Long>();
                while(cursor.getNext(key, val, null) == OperationStatus.SUCCESS) {
                    long localId = SerializerUtil.bytesToLong(key.getData());
                    String channelId = new String(val.getData(), UTF_8);
                    localChannelIds.put(channelId, localId);
                }
            } catch (Exception e) {
                localChannelIds = null;
                throw new DonkeyDaoException(e);
            } finally {
                if(cursor != null) {
                    cursor.close();
                }
            }
        }

        return localChannelIds;
    }

    @Override
    public void removeChannel(String channelId) {
        Long localChannelId = getLocalChannelIds().get(channelId);
        if (localChannelId == null) {
            return;
        }

        logger.debug(channelId + ": removing channel");

        transactionAlteredChannels = true;

        try {
            List<String> dbNames = new ArrayList<>();
            dbNames.add("d_ms" + localChannelId);
            dbNames.add("d_ma" + localChannelId);
            dbNames.add("d_mcm" + localChannelId);
            dbNames.add("d_mcm_columns");
            dbNames.add("d_mc" + localChannelId);
            dbNames.add("d_mm" + localChannelId);
            dbNames.add("d_mm_status" + localChannelId);
            dbNames.add("d_m" + localChannelId);

            for (String s : dbNames) {
                dbMap.remove(s).close();
                bdbJeEnv.removeDatabase(txn, s);
            }

            DatabaseEntry key = new DatabaseEntry(longToBytes(localChannelId));
            seqMap.remove(localChannelId).close();
            dbMap.get(TABLE_D_MESSAGE_SEQ).removeSequence(txn, key);
            
            dbMap.get(TABLE_D_CHANNELS).delete(txn, key);
            removedChannelIds.add(channelId);
        } catch (DatabaseException e) {
            throw new DonkeyDaoException(e);
        }
    }

    @Override
    public Long selectMaxLocalChannelId() {
        Cursor cursor = null;
        try {
            cursor = dbMap.get(TABLE_D_CHANNELS).openCursor(txn, null);
            long maxLocalChannelId = 0;
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry val = new DatabaseEntry();
            while(cursor.getNext(key, val, null) == OperationStatus.SUCCESS) {
                long tmp = bytesToLong(key.getData());
                if(tmp > maxLocalChannelId) {
                    maxLocalChannelId = tmp;
                }
            }
            
            return maxLocalChannelId;
        } catch (DatabaseException e) {
            throw new DonkeyDaoException(e);
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public void deleteAllMessages(String channelId) {
        logger.debug(channelId + ": deleting all messages");
        try {
            long cid = getLocalChannelId(channelId);
            boolean returnCount = false;
            bdbJeEnv.truncateDatabase(txn, "d_ma" + cid, returnCount);
            bdbJeEnv.truncateDatabase(txn, "d_mcm" + cid, returnCount);
            bdbJeEnv.truncateDatabase(txn, "d_mc" + cid, returnCount);
            bdbJeEnv.truncateDatabase(txn, "d_mm" + cid, returnCount);
            bdbJeEnv.truncateDatabase(txn, "d_m" + cid, returnCount);
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        }
    }

    @Override
    public void deleteMessageContent(String channelId, long messageId) {
        logger.debug(channelId + "/" + messageId + ": deleting content");
        long cid = getLocalChannelId(channelId);
        Database msgContentDb = dbMap.get("d_mc" + cid);
        deleteAllStartingwith(messageId, msgContentDb);
    }

    @Override
    public void deleteMessageContentByMetaDataIds(String channelId, long messageId, Set<Integer> metaDataIds) {
        logger.debug(channelId + "/" + messageId + ": deleting content by metadata IDs: " + String.valueOf(metaDataIds));

        try {
            long cid = getLocalChannelId(channelId);
            Database msgContentDb = dbMap.get("d_mc" + cid);
            byte[] keyData = buildPrimaryKeyOfConnectorMessage(messageId, 0);
            DatabaseEntry key = new DatabaseEntry(keyData);
            for(int mid : metaDataIds) {
                intToBytes(mid, keyData, 8);
                msgContentDb.delete(txn, key);
                // should the metadata table be checked to enforce ON DELETE RESTRICT?
            }
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        }
    }

    private void deleteMessageContentByMetaDataIdAndContentType(String channelId, long messageId, int metaDataId, ContentType contentType) {
        logger.debug(channelId + "/" + messageId + ": deleting content");

        try {
            long localChannelId = getLocalChannelId(channelId);
            DatabaseEntry key = new DatabaseEntry(buildPrimaryKeyOfMessageContent(messageId, metaDataId, contentType));
            Database msgContentDb = dbMap.get("d_mc" + localChannelId);
            msgContentDb.delete(txn, key);
        } catch (DatabaseException e) {
            throw new DonkeyDaoException(e);
        }
    }

    @Override
    public void deleteMessageAttachments(String channelId, long messageId) {
        logger.debug(channelId + "/" + messageId + ": deleting attachments");

        try {
            long localChannelId = getLocalChannelId(channelId);
            DatabaseEntry key = new DatabaseEntry(longToBytes(messageId));
            Database msgContentDb = dbMap.get("d_ma" + localChannelId);
            msgContentDb.delete(txn, key);
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        }
    }

    @Override
    public List<MetaDataColumn> getMetaDataColumns(String channelId) {
        try {
            List<MetaDataColumn> metaDataColumns = new ArrayList<MetaDataColumn>();

            Database metaColDb = dbMap.get(TABLE_D_META_COLUMNS);
            long localChannelId = getLocalChannelId(channelId);
            DatabaseEntry key = new DatabaseEntry(longToBytes(localChannelId));
            DatabaseEntry data = new DatabaseEntry();
            OperationStatus os = metaColDb.get(txn, key, data, LockMode.READ_COMMITTED);
            if(os == OperationStatus.SUCCESS) {
                MessageReader mr = readMessage(data.getData());
                CapMetadata.Reader atReader= mr.getRoot(CapMetadata.factory);

                for(CapMetadataColumn.Reader col : atReader.getColumns()) {
                    String name = col.getName().toString();
                    metaDataColumns.add(new MetaDataColumn(name, from(col.getType()), null));
                }
            }

            return metaDataColumns;
        } catch (Exception e) {
            throw new DonkeyDaoException("Failed to retrieve meta data columns", e);
        }
    }

    @Override
    public void removeMetaDataColumn(String channelId, String columnName) {
        logger.debug(channelId + "/" + ": removing custom meta data column (" + columnName + ")");
        MetaDataColumn mc = new MetaDataColumn();
        mc.setName(columnName.toUpperCase());
        addOrRemoveMetaDataColumn(channelId, mc, true);
    }

    @Override
    public long getMaxMessageId(String channelId) {
        return getMaxOrMinMessageId(channelId, true);
    }

    private long getMaxOrMinMessageId(String channelId, boolean max) {
        Cursor cursor = null;
        try {
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            long cid = getLocalChannelId(channelId);
            cursor = dbMap.get("d_m" + cid).openCursor(txn, null);
            OperationStatus os = null;
            if(max) {
                os = cursor.getLast(key, data, null);
            }
            else {
                os = cursor.getFirst(key, data, null);
            }
            
            long id = -1;
            if(os == OperationStatus.SUCCESS) {
                id = bytesToLong(key.getData());
            }
            return id;
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        }
        finally {
            if(cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public long getMinMessageId(String channelId) {
        return getMaxOrMinMessageId(channelId, false);
    }

    @Override
    public long getNextMessageId(String channelId) {
        try {
            long localChannelId = getLocalChannelId(channelId);
            return seqMap.get(localChannelId).get(txn, 1);
        } catch (DatabaseException e) {
            throw new DonkeyDaoException(e);
        }
    }

    @Override
    public List<Attachment> getMessageAttachment(String channelId, long messageId) {
        Cursor cursor = null;
        try {
            List<Attachment> attachments = new ArrayList<Attachment>();
            long cid = getLocalChannelId(channelId);
            Database atDb = dbMap.get("d_ma" + cid);
            cursor = atDb.openCursor(txn, null);
            DatabaseEntry key = new DatabaseEntry(longToBytes(messageId));
            DatabaseEntry data = new DatabaseEntry();
            OperationStatus os = cursor.getSearchKeyRange(key, data, null);
            if(os == OperationStatus.SUCCESS) {
                do {
                    long curPrefix = bytesToLong(key.getData());
                    if(curPrefix != messageId) {
                        break;
                    }
                    MessageReader mr = readMessage(data.getData());
                    CapAttachment.Reader at = mr.getRoot(CapAttachment.factory);
                    Attachment mcAt = new Attachment();
                    if(at.hasContent()) {
                        mcAt.setContent(at.getContent().toArray());
                    }
                    mcAt.setId(at.getId().toString());
                    mcAt.setType(at.getType().toString());
                    attachments.add(mcAt);
                }
                while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS);
            }

            return attachments;
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        }
        finally {
            if(cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public Attachment getMessageAttachment(String channelId, String attachmentId, Long messageId) {
        try {
            Attachment attachment = null;
            long cid = getLocalChannelId(channelId);
            Database atDb = dbMap.get("d_ma" + cid);
            DatabaseEntry key = new DatabaseEntry(buildPrimaryKeyOfAttachment(messageId, attachmentId));
            DatabaseEntry data = new DatabaseEntry();
            OperationStatus os = atDb.get(txn, key, data, LockMode.READ_COMMITTED);
            if(os == OperationStatus.SUCCESS) {
                MessageReader mr = readMessage(data.getData());
                CapAttachment.Reader at = mr.getRoot(CapAttachment.factory);
                attachment = new Attachment();
                if(at.hasContent()) {
                    attachment.setContent(at.getContent().toArray());
                }
                attachment.setId(at.getId().toString());
                attachment.setType(at.getType().toString());
            }
            else {
                attachment = new Attachment();
            }

            return attachment;
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        }
    }

    @Override
    public List<Message> getUnfinishedMessages(String channelId, String serverId, int limit, Long minMessageId) {
        Cursor conMsgStatusCursor = null;
        try {
            List<Message> messageList = new ArrayList<>();
            long cid = getLocalChannelId(channelId);
            Database msgDb = dbMap.get("d_m" + cid);
            Database conMsgStatusDb = dbMap.get("d_mm_status" + cid);
            CursorConfig cc = new CursorConfig();
            cc.setReadCommitted(true);
            conMsgStatusCursor = conMsgStatusDb.openCursor(txn, cc);

            byte[] conMsgKey = buildPrimaryKeyOfConnectorMessage(minMessageId, 0);
            DatabaseEntry key = new DatabaseEntry(conMsgKey);
            DatabaseEntry data = new DatabaseEntry();
            
            byte[] msgKey = new byte[8];
            OperationStatus os = conMsgStatusCursor.getSearchKeyRange(key, data, null);
            long tmpMsgId = -1;
            Message tmpMsg = null;
            if(os == OperationStatus.SUCCESS) {

                long foundMsgId = bytesToLong(key.getData());
                if(foundMsgId < minMessageId) {
                    return messageList;
                }

                Database conMsgDb = dbMap.get("d_mm" + cid);

                do {
                    char statusChar = (char) data.getData()[0];
                    if(statusChar == 'R') {
                        continue;
                    }

                    foundMsgId = bytesToLong(key.getData());
                    if(tmpMsgId != foundMsgId) {
                        longToBytes(foundMsgId, msgKey, 0);
                        key.setData(msgKey);
                        msgDb.get(txn, key, data, LockMode.READ_COMMITTED);
                        
                        MessageReader mr = readMessage(data.getData());
                        CapMessage.Reader m = mr.getRoot(CapMessage.factory);
                        if(!m.getProcessed()) {
                            if(limit <= 0) {
                                break;
                            }
                            
                            Message jm = getMessageFromResultSet(channelId, m);
                            messageList.add(jm);
                            tmpMsg = jm;
                            limit--;
                        } 
                    }

                    if(tmpMsg != null) {
                        int metaDataId = bytesToInt(key.getData(), 8);
                        longToBytes(foundMsgId, conMsgKey, 0);
                        intToBytes(metaDataId, conMsgKey, 8);
                        key.setData(conMsgKey);
                        conMsgDb.get(txn, key, data, LockMode.READ_COMMITTED);
                        CapConnectorMessage.Reader cm = readMessage(data.getData()).getRoot(CapConnectorMessage.factory);
                        ConnectorMessage conMsg = getConnectorMessageFromResultSet(channelId, cm, true, true);
                        conMsg.setStatus(Status.fromChar(statusChar));
                        tmpMsg.getConnectorMessages().put(metaDataId, conMsg);
                    }
                }
                while(conMsgStatusCursor.getNext(key, data, null) == OperationStatus.SUCCESS);
            }

            return messageList;
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        }
        finally {
            if(conMsgStatusCursor != null) {
                conMsgStatusCursor.close();
            }
        }
    }

    @Override
    public List<Message> getPendingConnectorMessages(String channelId, String serverId, int limit, Long minMessageId) {
        Cursor cursor = null;
        try {
            List<Message> messageList = new ArrayList<>();

            long cid = getLocalChannelId(channelId);
            Database conMsgStatusDb = dbMap.get("d_mm_status" + cid);
            Database conMsgDb = dbMap.get("d_mm" + cid);
            byte[] conMsgStatusKey = buildPrimaryKeyOfConnectorMessage(minMessageId, 1);
            DatabaseEntry key = new DatabaseEntry(conMsgStatusKey);
            DatabaseEntry data = new DatabaseEntry();
            cursor = conMsgStatusDb.openCursor(txn, null);
            OperationStatus os = cursor.getSearchKeyRange(key, data, null);
            long tmpMsgId = -1;
            Message tmpMsg = null;
            if(os == OperationStatus.SUCCESS) {
                long curMsgId = bytesToLong(key.getData(), 0);
                if(curMsgId < minMessageId) {
                    return messageList;
                }

                do {
                    if(data.getData()[0] != 'P') {
                        continue;
                    }
            
                    byte[] keyData = key.getData();
                    int metaDataId = bytesToInt(keyData, 8);
                    if(metaDataId == 0) {
                        continue;
                    }

                    DatabaseEntry conMsgKey = new DatabaseEntry(buildPrimaryKeyOfConnectorMessage(curMsgId, metaDataId));
                    os = conMsgDb.get(txn, conMsgKey, data, LockMode.READ_COMMITTED);

                    MessageReader mr = readMessage(data.getData());
                    CapConnectorMessage.Reader cm = mr.getRoot(CapConnectorMessage.factory);
                    curMsgId = bytesToLong(keyData);
                    if(tmpMsgId != curMsgId) {
                        if(limit <= 0) {
                            break;
                        }
                        
                        Message m = new Message();
                        m.setMessageId(curMsgId);
                        messageList.add(m);
                        tmpMsg = m;
                        limit--;
                    }
                    
                    ConnectorMessage connectorMessage = getConnectorMessageFromResultSet(channelId, cm, true, true);
                    connectorMessage.setStatus(Status.PENDING);
                    tmpMsg.getConnectorMessages().put(connectorMessage.getMetaDataId(), connectorMessage);
                }
                while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS);
            }
            
            return messageList;
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        }
        finally {
            if(cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public List<Message> getMessages(String channelId, List<Long> messageIds) {
        if (messageIds.size() > 1000) {
            throw new DonkeyDaoException("Only up to 1000 message Ids at a time are supported.");
        }

        List<Message> messageList = new ArrayList<>();
        Cursor conMsgCursor = null;
        try {
            // Cache all message content for all messages in the list
            Map<Long, Map<Integer, List<MessageContent>>> messageContentMap = getMessageContent(channelId, messageIds);
            // Cache all metadata maps for all messages in the list
            Map<Long, Map<Integer, Map<String, Object>>> metaDataMaps = getMetaDataMaps(channelId, messageIds);
            
            long cid = getLocalChannelId(channelId);
            Database msgDb = dbMap.get("d_m" + cid);
            Database conMsgDb = dbMap.get("d_mm" + cid);
            conMsgCursor = conMsgDb.openCursor(txn, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();

            Database conMsgStatus = dbMap.get("d_mm_status" + cid);
            DatabaseEntry statusKey = new DatabaseEntry();
            DatabaseEntry statusData = new DatabaseEntry();

            ConnectorMessage sourceConnectorMessage = null;
            for(long mid : messageIds) {
                key.setData(longToBytes(mid));
                OperationStatus os = msgDb.get(txn, key, data, LockMode.READ_COMMITTED);
                if(os == OperationStatus.SUCCESS) {
                    Message message = getMessageFromResultSet(channelId, data.getData());
                    messageList.add(message);
                    key.setData(buildPrimaryKeyOfConnectorMessage(mid, 0));
                    os = conMsgCursor.getSearchKey(key, data, null);
                    while(true) {
                        if(os != OperationStatus.SUCCESS) {
                            break;
                        }
                        if(bytesToLong(key.getData()) != mid) {
                            break;
                        }

                        CapConnectorMessage.Reader cm = readMessage(data.getData()).getRoot(CapConnectorMessage.factory);
                        // Create the connector from the result set without content or metadata map
                        ConnectorMessage connectorMessage = getConnectorMessageFromResultSet(channelId, cm, false, false);
                        statusKey.setData(key.getData());
                        conMsgStatus.get(txn, statusKey, statusData, LockMode.READ_COMMITTED);
                        char statusChar = (char)statusData.getData()[0];
                        Status s = Status.fromChar(statusChar);
                        connectorMessage.setStatus(s);

                        // Store the reference to the connector in the message
                        message.getConnectorMessages().put(connectorMessage.getMetaDataId(), connectorMessage);
                        
                        if (connectorMessage.getMetaDataId() == 0) {
                            // Store a reference to a message's source connector
                            sourceConnectorMessage = connectorMessage;
                        } else if (sourceConnectorMessage != null) {
                            // Load the destination's raw content and source map from the source
                            connectorMessage.setSourceMap(sourceConnectorMessage.getSourceMap());
                            MessageContent sourceEncoded = sourceConnectorMessage.getEncoded();
                            if (sourceEncoded != null) {
                                // If the source encoded exists, set it as the destination raw
                                MessageContent destinationRaw = new MessageContent(channelId, sourceEncoded.getMessageId(), connectorMessage.getMetaDataId(), ContentType.RAW, sourceEncoded.getContent(), sourceEncoded.getDataType(), sourceEncoded.isEncrypted());
                                connectorMessage.setRaw(destinationRaw);
                            }
                        }
                        
                        // Get this connector's message content from the cache and store it in the connector
                        Map<Integer, List<MessageContent>> connectorMessageContentMap = messageContentMap.get(connectorMessage.getMessageId());
                        if (connectorMessageContentMap != null) {
                            List<MessageContent> messageContents = connectorMessageContentMap.get(connectorMessage.getMetaDataId());
                            if (messageContents != null) {
                                loadMessageContent(connectorMessage, messageContents);
                            }
                        }
                        
                        // Get this connector's metadata map from the cache and store it in the connector
                        Map<Integer, Map<String, Object>> connectorMetaDataMap = metaDataMaps.get(connectorMessage.getMessageId());
                        if (connectorMetaDataMap != null) {
                            Map<String, Object> metaDataMap = connectorMetaDataMap.get(connectorMessage.getMetaDataId());
                            if (metaDataMap != null) {
                                connectorMessage.setMetaDataMap(metaDataMap);
                            }
                        }
                        
                        os = conMsgCursor.getNext(key, data, null);
                    }
                }
            }

            return messageList;
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        } finally {
            if(conMsgCursor != null) {
                conMsgCursor.close();
            }
        }
    }

    @Override
    public List<ConnectorMessage> getConnectorMessages(String channelId, String serverId, int metaDataId, Status status, int offset, int limit, Long minMessageId, Long maxMessageId) {
        List<ConnectorMessage> connectorMessages = new ArrayList<ConnectorMessage>();

        if (limit == 0) {
            return connectorMessages;
        }

        Cursor cursor = null;
        try {
            char sc = status.getStatusCode();
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            long cid = getLocalChannelId(channelId);
            if(minMessageId == null) {
                minMessageId = 0L;
            }

            if(maxMessageId == null) {
                maxMessageId = Long.MAX_VALUE;
            }

            Database conMsgDb = dbMap.get("d_mm" + cid);
            cursor = dbMap.get("d_mm_status" + cid).openCursor(txn, null);

            key.setData(buildPrimaryKeyOfConnectorMessage(minMessageId, metaDataId));
            OperationStatus os = cursor.getSearchKeyRange(key, data, null);
            if(os == OperationStatus.SUCCESS) {
                do {
                    long foundMsgId = bytesToLong(key.getData(), 0);
                    if(foundMsgId < minMessageId || foundMsgId > maxMessageId) {
                        break;
                    }

                    int foundMetaDataId = bytesToInt(key.getData(), 8);
                    if(metaDataId != foundMetaDataId) {
                        continue;
                    }
                    
                    if(sc == data.getData()[0]) {
                        conMsgDb.get(txn, key, data, LockMode.READ_COMMITTED);
                        CapConnectorMessage.Reader cm = readMessage(data.getData()).getRoot(CapConnectorMessage.factory);
                        ConnectorMessage cMsg = getConnectorMessageFromResultSet(channelId, cm, true, true);
                        cMsg.setStatus(Status.fromChar(sc));
                    }
                }
                while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS);
            }
            
            return connectorMessages;
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public List<ConnectorMessage> getConnectorMessages(String channelId, long messageId, Set<Integer> metaDataIds, boolean includeContent) {
        try {
            List<ConnectorMessage> connectorMessages = new ArrayList<ConnectorMessage>();

            long cid = getLocalChannelId(channelId);
            Database conMsgDb = dbMap.get("d_mm" + cid);
            Database conMsgStatusDb = dbMap.get("d_mm_status" + cid);

            byte[] buf = buildPrimaryKeyOfConnectorMessage(messageId, 0);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            for(int mtId : metaDataIds) {
                intToBytes(mtId, buf, 8);
                OperationStatus os = conMsgStatusDb.get(txn, key, data, LockMode.READ_COMMITTED);
                if(os == OperationStatus.SUCCESS) {
                    char sc = (char)data.getData()[0];
                    conMsgDb.get(txn, key, data, LockMode.READ_COMMITTED);
                    CapConnectorMessage.Reader cm = readMessage(data.getData()).getRoot(CapConnectorMessage.factory);
                    ConnectorMessage conMsg = getConnectorMessageFromResultSet(channelId, cm, includeContent, true);
                    conMsg.setStatus(Status.fromChar(sc));
                    connectorMessages.add(conMsg);
                }
            }

            return connectorMessages;
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        }
    }

    @Override
    public Map<Integer, ConnectorMessage> getConnectorMessages(String channelId, long messageId, List<Integer> metaDataIds) {
        Cursor cursor = null;

        try {
            Map<Integer, ConnectorMessage> connectorMessages = new HashMap<Integer, ConnectorMessage>();
            long cid = getLocalChannelId(channelId);
            Database conMsgDb = dbMap.get("d_mm" + cid);
            Database conMsgStatusDb = dbMap.get("d_mm_status" + cid);

            byte[] buf = buildPrimaryKeyOfConnectorMessage(messageId, 0);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            if(metaDataIds != null && !metaDataIds.isEmpty()) {
                for(int mtId : metaDataIds) {
                    intToBytes(mtId, buf, 8);
                    OperationStatus os = conMsgStatusDb.get(txn, key, data, LockMode.READ_COMMITTED);
                    if(os == OperationStatus.SUCCESS) {
                        char sc = (char)data.getData()[0];
                        conMsgDb.get(txn, key, data, LockMode.READ_COMMITTED);
                        CapConnectorMessage.Reader cm = readMessage(data.getData()).getRoot(CapConnectorMessage.factory);
                        ConnectorMessage conMsg = getConnectorMessageFromResultSet(channelId, cm, true, true);
                        conMsg.setStatus(Status.fromChar(sc));
                        connectorMessages.put(conMsg.getMetaDataId(), conMsg);
                    }
                }
            }
            else {
                cursor = conMsgStatusDb.openCursor(txn, null);
                OperationStatus os = cursor.getSearchKeyRange(key, data, null);
                if(os == OperationStatus.SUCCESS) {
                    do {
                        if(messageId != bytesToLong(key.getData())) {
                            break;
                        }
                        char sc = (char)data.getData()[0];
                        conMsgDb.get(txn, key, data, LockMode.READ_COMMITTED);
                        CapConnectorMessage.Reader cm = readMessage(data.getData()).getRoot(CapConnectorMessage.factory);
                        ConnectorMessage conMsg = getConnectorMessageFromResultSet(channelId, cm, true, true);
                        conMsg.setStatus(Status.fromChar(sc));
                        connectorMessages.put(conMsg.getMetaDataId(), conMsg);
                    }
                    while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS);
                }
            }

            return connectorMessages;
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public Map<Integer, Status> getConnectorMessageStatuses(String channelId, long messageId, boolean checkProcessed) {
        Cursor cursor = null;
        try {
            Map<Integer, Status> statusMap = new HashMap<Integer, Status>();

            long cid = getLocalChannelId(channelId);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            boolean skip = false;
            if(checkProcessed) {
                key.setData(longToBytes(messageId));
                OperationStatus os = dbMap.get("d_m" + cid).get(txn, key, data, LockMode.READ_COMMITTED);
                if(os == OperationStatus.SUCCESS) {
                    CapMessage.Reader cm = readMessage(data.getData()).getRoot(CapMessage.factory);
                    if(!cm.getProcessed()) {
                        skip = true;
                    }
                }
                else { // message doesn't exist
                    skip = true;
                }
            }

            if(!skip) {
                Database conMsgStatusDb = dbMap.get("d_mm_status" + cid);
                key.setData(buildPrimaryKeyOfConnectorMessage(messageId, 0));
                cursor = conMsgStatusDb.openCursor(txn, null);
                OperationStatus os = cursor.getSearchKeyRange(key, data, null);
                if(os == OperationStatus.SUCCESS) {
                    do {
                        byte[] buf = key.getData();
                        if(messageId != bytesToLong(buf)) {
                            break;
                        }
                        int metadataId = bytesToInt(buf, 8);
                        statusMap.put(metadataId, Status.fromChar((char)data.getData()[0]));
                    }
                    while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS);
                }
            }
            return statusMap;
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public int getConnectorMessageCount(String channelId, String serverId, int metaDataId, Status status) {
        Long cid = getLocalChannelIds().get(channelId);
        if (donkey.getDeployedChannels().get(channelId) != null || cid != null) {
            Cursor cursor = null;
            try {
                int count = 0;
                char sc = status.getStatusCode();
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry data = new DatabaseEntry();
                cursor = dbMap.get("d_mm_status" + cid).openCursor(txn, null);
                while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS) {
                    int foundMetaDataId = bytesToInt(key.getData(), 8);
                    if(metaDataId != foundMetaDataId) {
                        continue;
                    }
                    
                    if(sc == data.getData()[0]) {
                        count++;
                    }
                }
                
                return count;
            } catch (Exception e) {
                throw new DonkeyDaoException(e);
            } finally {
                if(cursor != null) {
                    cursor.close();
                }
            }
        } else {
            // the channel has never been deployed
            return 0;
        }
    }

    @Override
    public long getConnectorMessageMaxMessageId(String channelId, String serverId, int metaDataId, Status status) {
        Cursor cursor = null;
        try {
            long maxMsgId = 0;
            char sc = status.getStatusCode();
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            cursor = dbMap.get("d_mm_status").openCursor(txn, null);
            cursor.getLast(key, data, null);
            do {
                byte[] buf = key.getData();
                if(metaDataId != bytesToInt(buf, 8)) {
                    continue;
                }
                if(sc != (char)data.getData()[0]) {
                    continue;
                }
                
                maxMsgId = bytesToLong(buf);
                break;
            }
            while(cursor.getPrev(key, data, null) == OperationStatus.SUCCESS);
            return maxMsgId;
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public void addMetaDataColumn(String channelId, MetaDataColumn metaDataColumn) {
        logger.debug(channelId + ": adding custom meta data column (" + metaDataColumn.getName() + ")");
        addOrRemoveMetaDataColumn(channelId, metaDataColumn, false);
    }

    private void addOrRemoveMetaDataColumn(String channelId, MetaDataColumn metaDataColumn, boolean remove) {

        ReusableMessageBuilder rmb = null;
        try {
            List<MetaDataColumn> lst = new ArrayList<>();
            CapMetadata.Builder cb = null;

            Database metaColDb = dbMap.get(TABLE_D_META_COLUMNS);
            long cid = getLocalChannelId(channelId);
            DatabaseEntry key = new DatabaseEntry(longToBytes(cid));
            DatabaseEntry data = new DatabaseEntry();
            OperationStatus os = metaColDb.get(txn, key, data, LockMode.READ_COMMITTED);
            if(os == OperationStatus.SUCCESS) {
                MessageReader mr = readMessage(data.getData());
                CapMetadata.Reader atReader= mr.getRoot(CapMetadata.factory);

                rmb = objectPool.borrowObject(CapMetadata.class);
                rmb.getMb().setRoot(CapMetadata.factory, atReader);
                cb = (CapMetadata.Builder)rmb.getSb();
                
                for(CapMetadataColumn.Reader col : atReader.getColumns()) {
                    String name = col.getName().toString();
                    if(remove && name.equals(metaDataColumn.getName())) {
                        continue;
                    }
                    lst.add(new MetaDataColumn(name, from(col.getType()), col.getValue().toString()));
                }
            }
            else {
                rmb = objectPool.borrowObject(CapMetadata.class);
                cb = (CapMetadata.Builder)rmb.getSb();
                if(!remove) {
                    lst.add(metaDataColumn);
                }
            }
            
            StructList.Builder<CapMetadataColumn.Builder> colLstBuilder = cb.initColumns(lst.size());
            
            for (int i=0; i < lst.size(); i++) {
                CapMetadataColumn.Builder column = colLstBuilder.get(i);
                MetaDataColumn mc = lst.get(i);
                column.setName(mc.getName());
                column.setValue(mc.getMappingName());
                // @formatter:off
                switch (mc.getType()) {
                case STRING:
                    column.setType(CapMetadataColumn.Type.STRING);
                    break;
                case NUMBER:
                    column.setType(CapMetadataColumn.Type.NUMBER);
                    break;
                case BOOLEAN:
                    column.setType(CapMetadataColumn.Type.BOOLEAN);
                    break;
                case TIMESTAMP:
                    column.setType(CapMetadataColumn.Type.TIMESTAMP);
                    break;
                }
                // @formatter:on
            }
            
            writeMessageToEntry(rmb.getMb(), bufAlloc.buffer(), data);
            metaColDb.put(txn, key, data);
        } catch (Exception e) {
            throw new DonkeyDaoException("Failed to add meta-data column", e);
        } finally {
            if(rmb != null) {
                objectPool.returnObject(CapMetadata.class, rmb);
            }
        }
    }

    @Override
    public void createChannel(String channelId, long localChannelId) {
        logger.debug(channelId + ": creating channel");
        transactionAlteredChannels = true;

        try {
            Database db = dbMap.get(TABLE_D_CHANNELS);
            db.put(txn, new DatabaseEntry(SerializerUtil.longToBytes(localChannelId)), new DatabaseEntry(channelId.getBytes(UTF_8)));

            createTable("d_m" + localChannelId, txn); // createMessageTable
            createTable("d_mm" + localChannelId, txn); // createConnectorMessageTable
            createTable("d_mm_status" + localChannelId, txn); // createConnectorMessageTable
            createTable("d_mc" + localChannelId, txn); // createMessageContentTable
            createTable("d_mcm" + localChannelId, txn); // createMessageCustomMetaDataTable
            createTable("d_ma" + localChannelId, txn); // createMessageAttachmentTable
            createTable("d_ms" + localChannelId, txn); // createMessageStatisticsTable

            // createMessageSequence
            DatabaseEntry key = new DatabaseEntry(longToBytes(localChannelId));
            Sequence seq = dbMap.get(TABLE_D_MESSAGE_SEQ).openSequence(txn, key, SEQ_CONF);
            seqMap.put(localChannelId, seq);
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        }
    }

    private void createTable(String name) {
        createTable(name, null);
    }
    
    private void createTable(String name, Transaction localTxn) {
        createTable(name, localTxn, null);
    }

    private void createTable(String name, Transaction localTxn, DatabaseConfig dc) {
        if(dc == null) {
            dc = new DatabaseConfig();
        }
        // the below two config options are same for all Databases
        // and MUST be set or overwritten
        dc.setTransactional(true);
        dc.setAllowCreate(true);

        Database db = bdbJeEnv.openDatabase(localTxn, name, dc);
        dbMap.put(name, db);
    }

    @Override
    public void checkAndCreateChannelTables() {
        Map<String, Long> channelIds = getLocalChannelIds();
        List<String> channelTablesLst = new ArrayList<>();

        Database msq = dbMap.get(TABLE_D_MESSAGE_SEQ);
        DatabaseEntry seq = new DatabaseEntry(longToBytes(1));
        
        for (Long localChannelId : channelIds.values()) {
            channelTablesLst.add("d_m" + localChannelId); // "createMessageTable"
            channelTablesLst.add("d_mm" + localChannelId); // "createConnectorMessageTable"
            channelTablesLst.add("d_mc" + localChannelId); // "createMessageContentTable");
            channelTablesLst.add("d_mcm" + localChannelId); // "createMessageCustomMetaDataTable");
            channelTablesLst.add("d_ma" + localChannelId); // "createMessageAttachmentTable");
            channelTablesLst.add("d_ms" + localChannelId); // "createMessageStatisticsTable");

            // createMessageSequence
            DatabaseEntry key = new DatabaseEntry(longToBytes(localChannelId));
            msq.putNoOverwrite(txn, key, seq);
        }

        channelTablesLst.removeAll(bdbJeEnv.getDatabaseNames());

        try {
            for (String entry : channelTablesLst) {
                createTable(entry);
            }
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        }
    }

    @Override
    public void resetStatistics(String channelId, Integer metaDataId, Set<Status> statuses) {
        logger.debug(channelId + ": resetting statistics" + (metaDataId == null ? "" : (" for metadata id " + metaDataId)));

        ReusableMessageBuilder rmb = null;
        try {
            if (statuses == null || statuses.size() == 0) {
                return;
            }

            if(metaDataId == null) {
                metaDataId = -1; // channel stats
            }

            long cid = getLocalChannelId(channelId);
            Database conMsgStatsDb = dbMap.get("d_ms" + cid);
            DatabaseEntry key = new DatabaseEntry(buildPrimaryKeyOfStats(-1));
            DatabaseEntry data = new DatabaseEntry();
            
            OperationStatus os = conMsgStatsDb.get(txn, key, data, LockMode.READ_COMMITTED);
            CapStatistics.Builder cb = null;
            rmb = objectPool.borrowObject(CapStatistics.class);
            if(os == OperationStatus.SUCCESS) {
                CapStatistics.Reader cr = readMessage(data.getData()).getRoot(CapStatistics.factory);
                rmb.getMb().setRoot(CapStatistics.factory, cr);
                cb = (CapStatistics.Builder)rmb.getSb();
            }
            else {
                cb = (CapStatistics.Builder)rmb.getSb();
            }
            
            for(Status s : statuses) {
                switch (s) {
                case ERROR:
                    cb.setError(0);
                    break;

                case FILTERED:
                    cb.setFiltered(0);
                    break;
                case RECEIVED:
                    cb.setReceived(0);
                    break;
                case SENT:
                    cb.setSent(0);
                    break;
                }
            }
            
            writeMessageToEntry(rmb.getMb(), bufAlloc.buffer(), data);
            conMsgStatsDb.put(txn, key, data);
            
            if (!resetCurrentStats.containsKey(channelId)) {
                resetCurrentStats.put(channelId, new HashMap<Integer, Set<Status>>());
            }

            Map<Integer, Set<Status>> metaDataIds = resetCurrentStats.get(channelId);

            if (!metaDataIds.containsKey(metaDataId)) {
                metaDataIds.put(metaDataId, statuses);
            }

        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        } finally {
            if(rmb != null) {
                objectPool.returnObject(CapStatistics.class, rmb);
            }
        }
    }

    @Override
    public void resetAllStatistics(String channelId) {
        logger.debug(channelId + ": resetting all statistics (including lifetime)");

        try {
            long cid = getLocalChannelId(channelId);
            bdbJeEnv.truncateDatabase(txn, "d_ms" + cid, false);

            Set<Status> statuses = Statistics.getTrackedStatuses();

            Map<Integer, Set<Status>> metaDataIdsCurrent = resetCurrentStats.get(channelId);
            if (metaDataIdsCurrent == null) {
                metaDataIdsCurrent = new HashMap<Integer, Set<Status>>();
                resetCurrentStats.put(channelId, metaDataIdsCurrent);
            }

            Map<Integer, Map<Status, Long>> channelCurrentStats = currentStats.getChannelStats(channelId);
            if (channelCurrentStats != null) {
                for (Entry<Integer, Map<Status, Long>> channelEntry : channelCurrentStats.entrySet()) {
                    metaDataIdsCurrent.put(channelEntry.getKey(), statuses);
                }
            }

            Map<Integer, Set<Status>> metaDataIdsTotal = resetTotalStats.get(channelId);
            if (metaDataIdsTotal == null) {
                metaDataIdsTotal = new HashMap<Integer, Set<Status>>();
                resetTotalStats.put(channelId, metaDataIdsTotal);
            }

            Map<Integer, Map<Status, Long>> channelTotalStats = totalStats.getChannelStats(channelId);
            if (channelTotalStats != null) {
                for (Entry<Integer, Map<Status, Long>> channelEntry : channelTotalStats.entrySet()) {
                    metaDataIdsTotal.put(channelEntry.getKey(), statuses);
                }
            }
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        }
    }

    @Override
    public Statistics getChannelStatistics(String serverId) {
        return getChannelStatistics(serverId, false);
    }

    @Override
    public Statistics getChannelTotalStatistics(String serverId) {
        return getChannelStatistics(serverId, true);
    }

    private Statistics getChannelStatistics(String serverId, boolean total) {
        Map<String, Long> channelIds = getLocalChannelIds();
        Statistics statistics = new Statistics(!total);
        Cursor cursor = null;
        try {
            for (Map.Entry<String, Long> e : channelIds.entrySet()) {
                String channelId = e.getKey();
                long cid = e.getValue();
                cursor = dbMap.get("d_ms" + cid).openCursor(txn, null);
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry data = new DatabaseEntry();
                
                Map<Status, Long> stats = new HashMap<Status, Long>();
                
                while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS) {
                    CapStatistics.Reader cr = readMessage(data.getData()).getRoot(CapStatistics.factory);
                    long received = -1;
                    if(total) {
                        received = cr.getReceivedLifetime();
                    }
                    else {
                        received = cr.getReceived();
                    }
                    long filtered = -1;
                    if(total) {
                        filtered = cr.getFilteredLifetime();
                    }
                    else {
                        filtered = cr.getFiltered();
                    }
                    
                    long sent = -1;
                    if(total) {
                        sent = cr.getSentLifetime();
                    }
                    else {
                        sent = cr.getSent();
                    }
                    long error = -1;
                    if(total) {
                        error = cr.getErrorLifetime();
                    }
                    else {
                        error = cr.getError();
                    }
                    
                    stats.put(Status.RECEIVED, received);
                    stats.put(Status.FILTERED, filtered);
                    stats.put(Status.SENT, sent);
                    stats.put(Status.ERROR, error);
                    
                    int metaDataId = bytesToInt(key.getData(), 8);
                    statistics.overwrite(channelId, metaDataId, stats);
                }
            }
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        }
        finally {
            if(cursor != null) {
                cursor.close();
            }
        }

        return statistics;
    }

    @Override
    public void commit() {
        commit(true);
    }

    @Override
    public void commit(boolean durable) {
        logger.debug("Committing transaction" + (durable ? "" : " synchronously"));

        try {
            if(durable) {
                txn.commit();
            }
            else {
                txn.abort();
            }
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        }

        if (statisticsUpdater != null) {
            statisticsUpdater.update(transactionStats);
        }

        if (transactionAlteredChannels) {
            localChannelIds = null;
            transactionAlteredChannels = false;
        }

        if (currentStats != null) {
            // reset stats for any connectors that need to be reset
            for (Entry<String, Map<Integer, Set<Status>>> entry : resetCurrentStats.entrySet()) {
                String channelId = entry.getKey();
                Map<Integer, Set<Status>> metaDataIds = entry.getValue();

                for (Entry<Integer, Set<Status>> metaDataEntry : metaDataIds.entrySet()) {
                    Integer metaDataId = metaDataEntry.getKey();
                    Set<Status> statuses = metaDataEntry.getValue();

                    currentStats.resetStats(channelId, metaDataId, statuses);
                }
            }

            // Clear the reset stats map because we've just reset the stats
            resetCurrentStats.clear();

            // update the in-memory stats with the stats we just saved in storage
            currentStats.update(transactionStats);

            // remove the in-memory stats for any channels that were removed
            for (String channelId : removedChannelIds) {
                currentStats.remove(channelId);
            }
        }

        if (totalStats != null) {
            // reset stats for any connectors that need to be reset
            for (Entry<String, Map<Integer, Set<Status>>> entry : resetTotalStats.entrySet()) {
                String channelId = entry.getKey();
                Map<Integer, Set<Status>> metaDataIds = entry.getValue();

                for (Entry<Integer, Set<Status>> metaDataEntry : metaDataIds.entrySet()) {
                    Integer metaDataId = metaDataEntry.getKey();
                    Set<Status> statuses = metaDataEntry.getValue();

                    totalStats.resetStats(channelId, metaDataId, statuses);
                }
            }

            // Clear the reset stats map because we've just reset the stats
            resetTotalStats.clear();

            // update the in-memory total stats with the stats we just saved in storage
            totalStats.update(transactionStats);

            // remove the in-memory total stats for any channels that were removed
            for (String channelId : removedChannelIds) {
                totalStats.remove(channelId);
            }
        }

        transactionStats.clear();
    }

    @Override
    public void rollback() {
        logger.debug("Rolling back transaction");

        try {
            txn.abort();
            transactionStats.clear();
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        }
    }

    @Override
    public void close() {
        if(txn.isValid()) {
            txn.abort();
        }
    }

    @Override
    public boolean isClosed() {
        return !txn.isValid();
    }

    /*
     * Closes Statement or ResultSet, if needed
     */
    protected void closeDatabaseObjectIfNeeded(AutoCloseable dbObject) {
        // no op
    }

    /*
     * Closes any Statements or ResultSets, if needed
     */
    protected void closeDatabaseObjectsIfNeeded(List<AutoCloseable> dbObjects) {
        // no op
    }

    @Override
    public boolean initTableStructure() {
        if (!tableExists(TABLE_D_CHANNELS)) {
            logger.debug("Creating channels table");
            createTable(TABLE_D_CHANNELS);
            createTable(TABLE_D_MESSAGE_SEQ);
            createTable(TABLE_D_META_COLUMNS);
            return true;
        } else {
            return false;
        }
    }

    private boolean tableExists(String tableName) {
        try {
            return bdbJeEnv.getDatabaseNames().contains(tableName);
        } catch (DatabaseException e) {
            throw new DonkeyDaoException(e);
        }
    }

    public String getAsyncCommitCommand() {
        return asyncCommitCommand;
    }

    public void setAsyncCommitCommand(String asyncCommitCommand) {
        this.asyncCommitCommand = asyncCommitCommand;
    }

    private Message getMessageFromResultSet(String channelId, byte[] data) {
        try {
            MessageReader mr = readMessage(data);
            CapMessage.Reader msgReader = mr.getRoot(CapMessage.factory);
            return getMessageFromResultSet(channelId, msgReader);
        }
        catch (Exception e) {
            throw new DonkeyDaoException(e);
        }
    }
    
    private Message getMessageFromResultSet(String channelId, CapMessage.Reader msgReader) {
        try {
            Message message = new Message();
            message.setMessageId(msgReader.getMessageId());
            Calendar receivedDate = Calendar.getInstance();
            receivedDate.setTimeInMillis(msgReader.getReceivedDate());
            message.setReceivedDate(receivedDate);

            message.setChannelId(channelId);
            message.setProcessed(msgReader.getProcessed());
            message.setServerId(msgReader.getServerId().toString());
            if(msgReader.hasImportChannelId()) {
                message.setImportChannelId(msgReader.getImportChannelId().toString());
            }

            long importId = msgReader.getImportId();
            if(importId != -1) {
                message.setImportId(importId);
            }

            long originalId = msgReader.getOriginalId();
            if(originalId != -1) {
                message.setOriginalId(originalId);
            }

            return message;
        }
        catch (Exception e) {
            throw new DonkeyDaoException(e);
        }
    }

    private ConnectorMessage getConnectorMessageFromResultSet(String channelId, CapConnectorMessage.Reader cm, boolean includeContent, boolean includeMetaDataMap) {
        try {
            ConnectorMessage connectorMessage = new ConnectorMessage();
            connectorMessage.setChannelName(getDeployedChannelName(channelId));
            connectorMessage.setChannelId(channelId);

            int metaDataId = cm.getId();
            long messageId = cm.getMessageId();
            Calendar receivedDate = Calendar.getInstance();
            receivedDate.setTimeInMillis(cm.getReceivedDate());

            Calendar sendDate = null;
            if (cm.getSendDate() != 0) {
                sendDate = Calendar.getInstance();
                sendDate.setTimeInMillis(cm.getSendDate());
            }

            Calendar responseDate = null;
            if (cm.getResponseDate() != 0) {
                responseDate = Calendar.getInstance();
                responseDate.setTimeInMillis(cm.getResponseDate());
            }

            connectorMessage.setChannelName(getDeployedChannelName(channelId));
            connectorMessage.setMessageId(messageId);
            connectorMessage.setMetaDataId(metaDataId);
            connectorMessage.setChannelId(channelId);
            connectorMessage.setServerId(cm.getServerId().toString());
            connectorMessage.setConnectorName(cm.getConnectorName().toString());
            connectorMessage.setReceivedDate(receivedDate);
            //status is set in the caller connectorMessage.setStatus()
            connectorMessage.setSendAttempts(cm.getSendAttempts());
            connectorMessage.setSendDate(sendDate);
            connectorMessage.setResponseDate(responseDate);
            connectorMessage.setErrorCode(cm.getErrorCode());
            connectorMessage.setChainId(cm.getChainId());
            connectorMessage.setOrderId(cm.getOrderId());
            
            if (includeContent) {
                if (metaDataId > 0) {
                    // For destination connectors, retrieve and load any content that is stored on the source connector
                    loadMessageContent(connectorMessage, getDestinationMessageContentFromSource(channelId, messageId, metaDataId));
                }

                // Retrive all content for the connector and load it into the connector message
                loadMessageContent(connectorMessage, getMessageContent(channelId, messageId, metaDataId));
            }

            if (includeMetaDataMap) {
                connectorMessage.setMetaDataMap(getMetaDataMap(channelId, messageId, metaDataId));
            }
            return connectorMessage;
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        }
    }

    /**
     * Get all message content for a messageId and metaDataId
     */
    private List<MessageContent> getMessageContent(String channelId, long messageId, int metaDataId) {
        List<MessageContent> messageContents = new ArrayList<MessageContent>();
        Cursor cursor = null;
        try {
            long cid = getLocalChannelId(channelId);
            Database msgContentDb = dbMap.get("d_mc" + cid);
            byte[] msgContKey = buildPrimaryKeyOfMessageContent(messageId, metaDataId, ContentType.RAW);
            DatabaseEntry key = new DatabaseEntry(msgContKey);
            DatabaseEntry data = new DatabaseEntry();
            cursor = msgContentDb.openCursor(txn, null);
            
            OperationStatus os = cursor.getSearchKeyRange(key, data, null);
            if(os == OperationStatus.SUCCESS) {
                do {
                    if(bytesToLong(key.getData(), 0) != messageId) {
                        break;
                    }
                    messageContents.add(_getMessageContent(channelId, data.getData()));
                }
                while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS);
            }
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        }
        finally {
            if(cursor != null) {
                cursor.close();
            }
        }

        return messageContents;
    }

    private Map<Long, Map<Integer, List<MessageContent>>> getMessageContent(String channelId, List<Long> messageIds) {
        if (messageIds.size() > 1000) {
            throw new DonkeyDaoException("Only up to 1000 message Ids at a time are supported.");
        }

        Map<Long, Map<Integer, List<MessageContent>>> messageContentMap = new LinkedHashMap<Long, Map<Integer, List<MessageContent>>>();

        try {
            for(long messageId : messageIds) {
                Map<Integer, List<MessageContent>> connectorMessageContentMap = messageContentMap.get(messageId);
                if (connectorMessageContentMap == null) {
                    connectorMessageContentMap = new HashMap<Integer, List<MessageContent>>();
                    messageContentMap.put(messageId, connectorMessageContentMap);
                }

                List<MessageContent> messageContents = getMessageContent(channelId, messageId, 0);
                for(MessageContent mc : messageContents) {
                    int metaDataId = mc.getMetaDataId();
                    List<MessageContent> subLst = connectorMessageContentMap.get(metaDataId);
                    if (subLst == null) {
                        subLst = new ArrayList<MessageContent>();
                        connectorMessageContentMap.put(metaDataId, subLst);
                    }
                    subLst.add(mc);
                }

            }
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        }

        return messageContentMap;
    }

    /**
     * Get all content for a destination connector that is stored with the source connector
     */
    private List<MessageContent> getDestinationMessageContentFromSource(String channelId, long messageId, int metaDataId) {
        List<MessageContent> messageContents = new ArrayList<MessageContent>();

        try {
            long cid = getLocalChannelId(channelId);
            Database msgContentDb = dbMap.get("d_mc" + cid);
            byte[] msgContKey = buildPrimaryKeyOfMessageContent(messageId, metaDataId, ContentType.ENCODED);
            DatabaseEntry key = new DatabaseEntry(msgContKey);
            DatabaseEntry data = new DatabaseEntry();
            OperationStatus os = msgContentDb.get(txn, key, data, LockMode.READ_COMMITTED);
            if(os == OperationStatus.SUCCESS) {
                MessageContent mc = _getMessageContent(channelId, data.getData());
                if (mc.getContentType() == ContentType.ENCODED) {
                    mc.setContentType(ContentType.RAW);
                }

                messageContents.add(mc);
            }
            else {
                intToBytes(ContentType.SOURCE_MAP.getContentTypeCode(), msgContKey, 12);
                key.setData(msgContKey);
                os = msgContentDb.get(txn, key, data, LockMode.READ_COMMITTED);
                if(os == OperationStatus.SUCCESS) {
                    MessageContent mc = _getMessageContent(channelId, data.getData());
                    if (mc.getContentType() == ContentType.ENCODED) {
                        mc.setContentType(ContentType.RAW);
                    }

                    messageContents.add(mc);
                }
            }
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        }

        return messageContents;
    }

    private MessageContent _getMessageContent(String channelId, byte[] data) throws Exception {
        MessageReader mr = readMessage(data);
        CapMessageContent.Reader mcr = mr.getRoot(CapMessageContent.factory);
        ContentType contentType = fromCapContentType(mcr.getContentType());
        String content = mcr.getContent().toString();
        boolean encrypted = mcr.getEncrypted();
        if ((decryptData || alwaysDecrypt.contains(contentType)) && encrypted && encryptor != null) {
            content = encryptor.decrypt(content);
            encrypted = false;
        }

        return new MessageContent(channelId, mcr.getMessageId(), mcr.getMetaDataId(), contentType, content, mcr.getDataType().toString(), encrypted);
    }

    /**
     * Load message content into the connector message based on the content type
     */
    private void loadMessageContent(ConnectorMessage connectorMessage, List<MessageContent> messageContents) {
        for (MessageContent messageContent : messageContents) {
            switch (messageContent.getContentType()) {
                case RAW:
                    connectorMessage.setRaw(messageContent);
                    break;
                case PROCESSED_RAW:
                    connectorMessage.setProcessedRaw(messageContent);
                    break;
                case TRANSFORMED:
                    connectorMessage.setTransformed(messageContent);
                    break;
                case ENCODED:
                    connectorMessage.setEncoded(messageContent);
                    break;
                case SENT:
                    connectorMessage.setSent(messageContent);
                    break;
                case RESPONSE:
                    connectorMessage.setResponse(messageContent);
                    break;
                case RESPONSE_TRANSFORMED:
                    connectorMessage.setResponseTransformed(messageContent);
                    break;
                case PROCESSED_RESPONSE:
                    connectorMessage.setProcessedResponse(messageContent);
                    break;
                case CONNECTOR_MAP:
                    connectorMessage.setConnectorMapContent(getMapContentFromMessageContent(messageContent));
                    break;
                case CHANNEL_MAP:
                    connectorMessage.setChannelMapContent(getMapContentFromMessageContent(messageContent));
                    break;
                case RESPONSE_MAP:
                    connectorMessage.setResponseMapContent(getMapContentFromMessageContent(messageContent));
                    break;
                case PROCESSING_ERROR:
                    connectorMessage.setProcessingErrorContent(getErrorContentFromMessageContent(messageContent));
                    break;
                case POSTPROCESSOR_ERROR:
                    connectorMessage.setPostProcessorErrorContent(getErrorContentFromMessageContent(messageContent));
                    break;
                case RESPONSE_ERROR:
                    connectorMessage.setResponseErrorContent(getErrorContentFromMessageContent(messageContent));
                    break;
                case SOURCE_MAP:
                    connectorMessage.setSourceMapContent(getMapContentFromMessageContent(messageContent));
                    break;
            }
        }
    }

    private MapContent getMapContentFromMessageContent(MessageContent content) {
        if (content == null) {
            return new MapContent(new HashMap<String, Object>(), false);
        } else if (StringUtils.isBlank(content.getContent())) {
            return new MapContent(new HashMap<String, Object>(), true);
        }

        return new MapContent(MapUtil.deserializeMap(serializerProvider.getSerializer(content.getMetaDataId()), content.getContent()), true);
    }

    private ErrorContent getErrorContentFromMessageContent(MessageContent content) {
        if (content == null) {
            return new ErrorContent(null, false);
        }

        return new ErrorContent(content.getContent(), true);
    }

    private Map<String, Object> getMetaDataMap(String channelId, long messageId, int metaDataId) {
        try {
            Map<String, Object> metaDataMap = null;

            long cid = getLocalChannelId(channelId);
            Database metadataDb = dbMap.get("d_mcm" + cid);
            DatabaseEntry key = new DatabaseEntry(buildPrimaryKeyOfMetadata(messageId, metaDataId)); 
            DatabaseEntry data = new DatabaseEntry(); 
            OperationStatus os = metadataDb.get(txn, key, data, LockMode.READ_COMMITTED);
            if (os == OperationStatus.SUCCESS) {
                metaDataMap = _readMetadataMap(data.getData());
            }
            else {
                metaDataMap = new HashMap<String, Object>();
            }
            
            return metaDataMap;
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        }
    }

    private Map<String, Object> _readMetadataMap(byte[] data) throws Exception {
        Map<String, Object> metaDataMap = new HashMap<String, Object>();
        MessageReader mr = readMessage(data);
        CapMetadata.Reader cm = mr.getRoot(CapMetadata.factory);
        if(cm.hasColumns()) {
            for(CapMetadataColumn.Reader c : cm.getColumns()) {
                String name = c.getName().toString();
                String storedVal = c.getValue().toString();
                Object val = null;
                switch(c.getType()) {
                case STRING:
                    val = storedVal;
                    break;
                case BOOLEAN:
                    val = Boolean.parseBoolean(storedVal);
                    break;
                case NUMBER:
                    val = new BigDecimal(storedVal);
                    break;
                case TIMESTAMP:
                    Calendar cal = Calendar.getInstance();
                    cal.setTimeInMillis(Long.parseLong(storedVal));
                    break;
                default:
                    throw new Exception("Unrecognized MetaDataColumnType " + c.getType());
                }
                metaDataMap.put(name, val);
            }
        }
        
        return metaDataMap;
    }

    private Map<Integer, Map<String, Object>> _readAllMetadataOfMessageId(Database metadataDb, long messageId) throws Exception {
        Map<Integer, Map<String, Object>> connectorMetaDataMap = null;
        Cursor metadataCursor = null;
        try {
            metadataCursor = metadataDb.openCursor(txn, null);
            byte[] metaKeyPrefix = new byte[12];
            longToBytes(messageId, metaKeyPrefix, 0);
            DatabaseEntry key = new DatabaseEntry(metaKeyPrefix); 
            DatabaseEntry data = new DatabaseEntry(); 
            
            OperationStatus os = metadataCursor.getSearchKeyRange(key, data, null);
            if(os == OperationStatus.SUCCESS) {
                connectorMetaDataMap = new HashMap<Integer, Map<String, Object>>();
                do {
                    long msgId = bytesToLong(key.getData(), 0);
                    if(msgId != messageId) {
                        break;
                    }
                    
                    int metaId = bytesToInt(key.getData(), 8);
                    Map<String, Object> mmap = _readMetadataMap(data.getData());
                    connectorMetaDataMap.put(metaId, mmap);
                }
                while(metadataCursor.getNext(key, data, null) == OperationStatus.SUCCESS);
            }
        }
        finally {
            if(metadataCursor != null) {
                metadataCursor.close();
            }
        }
        return connectorMetaDataMap;
    }

    private Map<Long, Map<Integer, Map<String, Object>>> getMetaDataMaps(String channelId, List<Long> messageIds) {
        if (messageIds.size() > 1000) {
            throw new DonkeyDaoException("Only up to 1000 message Ids at a time are supported.");
        }

        Map<Long, Map<Integer, Map<String, Object>>> metaDataMaps = new HashMap<Long, Map<Integer, Map<String, Object>>>();

        try {
            long cid = getLocalChannelId(channelId);
            Database metadataDb = dbMap.get("d_mcm" + cid);
            for(long mid : messageIds) {
                Map<Integer, Map<String, Object>> connectorMetaDataMap = _readAllMetadataOfMessageId(metadataDb, mid);
                if(connectorMetaDataMap != null) {
                    metaDataMaps.put(mid, connectorMetaDataMap);
                }
            }

            return metaDataMaps;
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        }
    }

    protected void close(Statement statement) {
    }

    protected void close(ResultSet resultSet) {
    }

    private static String sqlTypeToString(int sqlType) {
        switch (sqlType) {
            case Types.ARRAY:
                return "ARRAY";
            case Types.BIGINT:
                return "BIGINT";
            case Types.BINARY:
                return "BINARY";
            case Types.BIT:
                return "BIT";
            case Types.BLOB:
                return "BLOB";
            case Types.BOOLEAN:
                return "BOOLEAN";
            case Types.CHAR:
                return "CHAR";
            case Types.CLOB:
                return "CLOB";
            case Types.DATALINK:
                return "DATALINK";
            case Types.DATE:
                return "DATE";
            case Types.DECIMAL:
                return "DECIMAL";
            case Types.DISTINCT:
                return "DISTINCT";
            case Types.DOUBLE:
                return "DOUBLE";
            case Types.FLOAT:
                return "FLOAT";
            case Types.INTEGER:
                return "INTEGER";
            case Types.JAVA_OBJECT:
                return "JAVA_OBJECT";
            case Types.LONGNVARCHAR:
                return "LONGNVARCHAR";
            case Types.LONGVARBINARY:
                return "LONGVARBINARY";
            case Types.LONGVARCHAR:
                return "LONGVARCHAR";
            case Types.NCHAR:
                return "NCHAR";
            case Types.NCLOB:
                return "NCLOB";
            case Types.NULL:
                return "NULL";
            case Types.NUMERIC:
                return "NUMERIC";
            case Types.NVARCHAR:
                return "NVARCHAR";
            case Types.OTHER:
                return "OTHER";
            case Types.REAL:
                return "REAL";
            case Types.REF:
                return "REF";
            case Types.ROWID:
                return "ROWID";
            case Types.SMALLINT:
                return "SMALLINT";
            case Types.SQLXML:
                return "SQLXML";
            case Types.STRUCT:
                return "STRUCT";
            case Types.TIME:
                return "TIME";
            case Types.TIMESTAMP:
                return "TIMESTAMP";
            case Types.TINYINT:
                return "TINYINT";
            case Types.VARBINARY:
                return "VARBINARY";
            case Types.VARCHAR:
                return "VARCHAR";
            default:
                return "UNKNOWN";
        }
    }

    protected long getLocalChannelId(String channelId) {
        Channel channel = donkey.getDeployedChannels().get(channelId);

        if (channel == null) {
            Long localChannelId = getLocalChannelIds().get(channelId);

            if (localChannelId == null) {
                throw new ChannelDoesNotExistException(channelId);
            } else {
                return localChannelId;
            }
        } else {
            return channel.getLocalChannelId();
        }
    }

    protected String getDeployedChannelName(String channelId) {
        Channel channel = donkey.getDeployedChannels().get(channelId);
        return channel != null ? channel.getName() : "";
    }
    
    private byte[] buildPrimaryKeyOfMessageContent(long messageId, int metaDataId, ContentType ct) {
        byte[] buf = new byte[16]; // MESSAGE_ID, METADATA_ID, CONTENT_TYPE
        longToBytes(messageId, buf, 0);
        intToBytes(metaDataId, buf, 8);
        intToBytes(ct.getContentTypeCode(), buf, 12);
        
        return buf;
    }
    
    private byte[] buildPrimaryKeyOfConnectorMessage(long messageId, int metaDataId) {
        byte[] buf = new byte[12]; // MESSAGE_ID, METADATA_ID
        longToBytes(messageId, buf, 0);
        intToBytes(metaDataId, buf, 8);
        
        return buf;
    }

    private byte[] buildPrimaryKeyOfMetadata(long messageId, int metaDataId) {
        byte[] buf = new byte[12]; // MESSAGE_ID, METADATA_ID
        longToBytes(messageId, buf, 0);
        intToBytes(metaDataId, buf, 8);
        
        return buf;
    }

    private byte[] buildPrimaryKeyOfStats(int metadataId) {
        byte[] tmp = statsServerId.getBytes(UTF_8);
        byte[] data = new byte[tmp.length + 4];
        System.arraycopy(tmp, 0, data, 0, tmp.length);
        intToBytes(metadataId, data, tmp.length);
        
        return data;
    }

    private byte[] buildPrimaryKeyOfAttachment(long messageId, String attachmentId) {
        byte[] tmp = attachmentId.getBytes(UTF_8);
        byte[] data = new byte[8 + tmp.length];
        longToBytes(messageId, data, 0);
        System.arraycopy(tmp, 0, data, 8, tmp.length);
        
        return data;
    }

    private long deleteAllStartingwith(long keyPrefixId, Database db) {
        Cursor cursor = db.openCursor(null, null);
        DatabaseEntry key = new DatabaseEntry(longToBytes(keyPrefixId));
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus os = cursor.getSearchKeyRange(key, data, null);
        long total = 0;
        if(os == OperationStatus.SUCCESS) {
            do {
                //System.out.println(bytesToLong(key.getData()) + " - " + bytesToLong(data.getData()));
                long curPrefix = bytesToLong(key.getData());
                if(curPrefix != keyPrefixId) {
                    break;
                }
                cursor.delete();
                total++;
            }
            while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS);
        }
        cursor.close();
        
        return total;
    }
    
    private MetaDataColumnType from(CapMetadataColumn.Type t) {
        switch (t) {
        case STRING:
            return MetaDataColumnType.STRING;
        case BOOLEAN:
            return MetaDataColumnType.BOOLEAN;
        case NUMBER:
            return MetaDataColumnType.NUMBER;
        case TIMESTAMP:
            return MetaDataColumnType.TIMESTAMP;
        }
        
        throw new IllegalArgumentException("unknown capnp metadata column type " + t);
    }
    
    private boolean isEquals(byte[] data, org.capnproto.Text.Reader reader) {
        if(data.length != reader.size) {
            return false;
        }

        int o = reader.offset;
        for(int i = 0; i < data.length; i++) {
            if(data[i] != reader.buffer.get(o+i)) {
                return false;
            }
        }
        
        return true;
    }
    
    private boolean attachUnfinishedConnectorMessagesTo(Message m, long localChannelId, Cursor conMsgCursor, boolean includeContent, boolean includeMetaDataMap) throws Exception {
        long mid = m.getMessageId();
        byte[] pk = buildPrimaryKeyOfConnectorMessage(mid, 0);
        DatabaseEntry key = new DatabaseEntry(pk);
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus os = conMsgCursor.getSearchKeyRange(key, data, null);
        
        Database conMsgStatus = dbMap.get("d_mm_status" + localChannelId);
        DatabaseEntry statusKey = new DatabaseEntry(pk);
        DatabaseEntry statusData = new DatabaseEntry();

        boolean attached = false;
        if(os == OperationStatus.SUCCESS) {
            do {
                long curMid = bytesToLong(key.getData());
                if(curMid != mid) {
                    break;
                }

                int metaDataId = bytesToInt(key.getData(), 8);
                intToBytes(metaDataId, pk, 8);
                statusKey.setData(pk);
                os = conMsgStatus.get(txn, statusKey, statusData, LockMode.READ_COMMITTED);
                char statusChar = (char)statusData.getData()[0];
                if(os == OperationStatus.SUCCESS && Status.RECEIVED.getStatusCode() != statusChar) {
                    MessageReader mr = readMessage(data.getData());
                    CapConnectorMessage.Reader cm = mr.getRoot(CapConnectorMessage.factory);
                    ConnectorMessage conMsg = getConnectorMessageFromResultSet(m.getChannelId(), cm, includeContent, includeMetaDataMap);
                    
                    Status s = Status.fromChar(statusChar);
                    conMsg.setStatus(s);
                    m.getConnectorMessages().put(metaDataId, conMsg);
                    attached = true;
                }
            }
            while(conMsgCursor.getNext(key, data, null) == OperationStatus.SUCCESS);
        }
        
        // do not close the cursor
        return attached;
    }

    private CapMessageContent.CapContentType toCapContentType(ContentType contentType) {
        CapContentType cct = null;
        switch (contentType) {
        case CHANNEL_MAP:
            cct = CHANNELMAP;
            break;
        case CONNECTOR_MAP:
            cct = CONNECTORMAP;
            break;
        case ENCODED:
            cct = ENCODED;
            break;
        case POSTPROCESSOR_ERROR:
            cct = POSTPROCESSORERROR;
            break;
        case PROCESSED_RAW:
            cct = PROCESSEDRAW;
            break;
        case PROCESSED_RESPONSE:
            cct = PROCESSEDRESPONSE;
            break;
        case PROCESSING_ERROR:
            cct = PROCESSINGERROR;
            break;
        case RAW:
            cct = RAW;
            break;
        case RESPONSE:
            cct = RESPONSE;
            break;
        case RESPONSE_ERROR:
            cct = RESPONSEERROR;
            break;
        case RESPONSE_MAP:
            cct = RESPONSEMAP;
            break;
        case RESPONSE_TRANSFORMED:
            cct = RESPONSETRANSFORMED;
            break;
        case SENT:
            cct = SENT;
            break;
        case SOURCE_MAP:
            cct = SOURCEMAP;
            break;
        case TRANSFORMED:
            cct = TRANSFORMED;
            break;
        default:
            throw new IllegalArgumentException("unknown content type " + contentType);
        }
        
        return cct;
    }
    
    private ContentType fromCapContentType(CapMessageContent.CapContentType cct) {
        ContentType contentType = null;
        switch (cct) {
        case CHANNELMAP:
            contentType = ContentType.CHANNEL_MAP;
            break;
        case CONNECTORMAP:
            contentType = ContentType.CONNECTOR_MAP;
            break;
        case ENCODED:
            contentType = ContentType.ENCODED;
            break;
        case POSTPROCESSORERROR:
            contentType = ContentType.POSTPROCESSOR_ERROR;
            break;
        case PROCESSEDRAW:
            contentType = ContentType.PROCESSED_RAW;
            break;
        case PROCESSEDRESPONSE:
            contentType = ContentType.PROCESSED_RESPONSE;
            break;
        case PROCESSINGERROR:
            contentType = ContentType.PROCESSING_ERROR;
            break;
        case RAW:
            contentType = ContentType.RAW;
            break;
        case RESPONSE:
            contentType = ContentType.RESPONSE;
            break;
        case RESPONSEERROR:
            contentType = ContentType.RESPONSE_ERROR;
            break;
        case RESPONSEMAP:
            contentType = ContentType.RESPONSE_MAP;
            break;
        case RESPONSETRANSFORMED:
            contentType = ContentType.RESPONSE_TRANSFORMED;
            break;
        case SENT:
            contentType = ContentType.SENT;
            break;
        case SOURCEMAP:
            contentType = ContentType.SOURCE_MAP;
            break;
        case TRANSFORMED:
            contentType = ContentType.TRANSFORMED;
            break;

        default:
            throw new IllegalArgumentException("unknown Cap content type " + cct);
        }
        
        return contentType;
    }    
}
