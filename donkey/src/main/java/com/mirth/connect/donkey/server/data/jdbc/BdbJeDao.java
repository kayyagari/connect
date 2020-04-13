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
import static com.mirth.connect.donkey.util.SerializerUtil.bytesToLong;
import static com.mirth.connect.donkey.util.SerializerUtil.intToBytes;
import static com.mirth.connect.donkey.util.SerializerUtil.longToBytes;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.log4j.Logger;
import org.capnproto.MessageReader;
import org.capnproto.SerializePacked;
import org.capnproto.StructList;

import com.mirth.connect.donkey.model.channel.MetaDataColumn;
import com.mirth.connect.donkey.model.channel.MetaDataColumnType;
import com.mirth.connect.donkey.model.message.CapnpModel.CapAttachment;
import com.mirth.connect.donkey.model.message.CapnpModel.CapConnectorMessage;
import com.mirth.connect.donkey.model.message.CapnpModel.CapConnectorMessage.CapStatus;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMessage;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent.CapContentType;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMetadata;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMetadataColumn;
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
import static com.mirth.connect.donkey.model.message.CapnpModel.CapConnectorMessage.CapStatus.*;

public class BdbJeDao implements DonkeyDao {
    private Donkey donkey;
    private Connection connection;
    private QuerySource querySource;
    private PreparedStatementSource statementSource;
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

    private static final String TABLE_D_CHANNELS = "d_channels";
    private static final String TABLE_D_MESSAGE_SEQ = "d_msq";

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

    protected BdbJeDao(Donkey donkey, Connection connection, QuerySource querySource, PreparedStatementSource statementSource, SerializerProvider serializerProvider, boolean encryptData, boolean decryptData, StatisticsUpdater statisticsUpdater, Statistics currentStats, Statistics totalStats, String statsServerId) {
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
            if(message.getImportId() != null) {
                mb.setImportId(message.getImportId());
            }
            mb.setMessageId(message.getMessageId());
            if(message.getOriginalId() != null) {
                mb.setOriginalId(message.getOriginalId());
            }
            mb.setProcessed(message.isProcessed());
            mb.setReceivedDate(message.getReceivedDate().getTimeInMillis());
            mb.setServerId(message.getServerId());
            
            CapnpOutputStream out = new CapnpOutputStream(bufAlloc.buffer());
            SerializePacked.write(out, rmb.getMb());

            long localChannelId = getLocalChannelId(message.getChannelId());
            DatabaseEntry key = new DatabaseEntry(longToBytes(message.getMessageId()));
            dbMap.get("d_m" + localChannelId).put(txn, key, out.getData());
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

        PreparedStatement statement = null;
        ReusableMessageBuilder rmb = null;
        try {
            DatabaseEntry key = new DatabaseEntry(buildPrimaryKeyOfConnectorMessage(connectorMessage.getMessageId(), connectorMessage.getMetaDataId()));
            long cid = getLocalChannelId(connectorMessage.getChannelId());
            DatabaseEntry data = new DatabaseEntry();
            Database cmDb = dbMap.get("d_mm" + cid);
            OperationStatus os = cmDb.get(txn, key, data, LockMode.READ_COMMITTED);
            if(os == OperationStatus.SUCCESS) {
                CapnpInputStream in = new CapnpInputStream(data.getData());
                MessageReader mr = SerializePacked.read(in);
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
                
                CapnpOutputStream out = new CapnpOutputStream(bufAlloc.buffer());
                SerializePacked.write(out, rmb.getMb());
                data = out.getData();
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

            cb.setChannelId(channelId);
            cb.setContent(content);
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
            }

            cb.setContentType(cct);
            cb.setDataType(dataType);
            cb.setEncrypted(encrypted);
            cb.setMessageId(messageId);
            cb.setMetaDataId(metaDataId);

            CapnpOutputStream out = new CapnpOutputStream(bufAlloc.buffer());
            SerializePacked.write(out, rmb.getMb());

            long localChannelId = getLocalChannelId(channelId);
            DatabaseEntry key = new DatabaseEntry(buildPrimaryKeyOfMessageContent(messageId, metaDataId, contentType));
            DatabaseEntry data = out.getData();
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

        PreparedStatement statement = null;

        try {
            /*
             * Indicates whether case statements are used in the update statement, in which case the
             * number of bound parameters for each statistic will double.
             */
            boolean usingCase = false;

            if (metaDataId == null) {
                if (querySource.queryExists("updateChannelStatisticsWithCase")) {
                    usingCase = true;
                    statement = prepareStatement("updateChannelStatisticsWithCase", channelId);
                } else {
                    statement = prepareStatement("updateChannelStatistics", channelId);
                }
            } else {
                if (querySource.queryExists("updateConnectorStatisticsWithCase")) {
                    usingCase = true;
                    statement = prepareStatement("updateConnectorStatisticsWithCase", channelId);
                } else {
                    statement = prepareStatement("updateConnectorStatistics", channelId);
                }
            }

            // Keep track of the index since it will change depending on whether case statements are used
            int paramIndex = 1;

            if (usingCase) {
                statement.setLong(paramIndex++, received);
                statement.setLong(paramIndex++, received);
                statement.setLong(paramIndex++, received);
                statement.setLong(paramIndex++, received);
                statement.setLong(paramIndex++, filtered);
                statement.setLong(paramIndex++, filtered);
                statement.setLong(paramIndex++, filtered);
                statement.setLong(paramIndex++, filtered);
                statement.setLong(paramIndex++, sent);
                statement.setLong(paramIndex++, sent);
                statement.setLong(paramIndex++, sent);
                statement.setLong(paramIndex++, sent);
                statement.setLong(paramIndex++, error);
                statement.setLong(paramIndex++, error);
                statement.setLong(paramIndex++, error);
                statement.setLong(paramIndex++, error);
            } else {
                statement.setLong(paramIndex++, received);
                statement.setLong(paramIndex++, received);
                statement.setLong(paramIndex++, filtered);
                statement.setLong(paramIndex++, filtered);
                statement.setLong(paramIndex++, sent);
                statement.setLong(paramIndex++, sent);
                statement.setLong(paramIndex++, error);
                statement.setLong(paramIndex++, error);
            }

            if (metaDataId != null) {
                statement.setInt(paramIndex++, metaDataId);
                statement.setString(paramIndex++, statsServerId);
            } else {
                statement.setString(paramIndex++, statsServerId);
            }

            if (statement.executeUpdate() == 0) {
                closeDatabaseObjectIfNeeded(statement);
                statement = prepareStatement("insertChannelStatistics", channelId);

                if (metaDataId == null) {
                    statement.setNull(1, Types.INTEGER);
                } else {
                    statement.setInt(1, metaDataId);
                }

                statement.setString(2, statsServerId);
                statement.setLong(3, received);
                statement.setLong(4, received);
                statement.setLong(5, filtered);
                statement.setLong(6, filtered);
                statement.setLong(7, sent);
                statement.setLong(8, sent);
                statement.setLong(9, error);
                statement.setLong(10, error);
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            closeDatabaseObjectIfNeeded(statement);
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
            cab.setId(attachment.getAttachmentId());
            cab.setType(attachment.getType());
            cab.setAttachmentSize(attachment.getContent().length);
            cab.setMessageId(messageId);
            
            CapnpOutputStream out = new CapnpOutputStream(bufAlloc.buffer());
            SerializePacked.write(out, rmb.getMb());
            DatabaseEntry key = new DatabaseEntry(buildPrimaryKeyOfAttachment(messageId, attachment.getId()));
            DatabaseEntry data = out.getData();
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
                
                CapnpOutputStream out = new CapnpOutputStream(bufAlloc.buffer());
                SerializePacked.write(out, rmb.getMb());
                long cid = getLocalChannelId(connectorMessage.getChannelId());
                DatabaseEntry key = new DatabaseEntry(longToBytes(connectorMessage.getMessageId()));
                DatabaseEntry data = out.getData();
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
            CapConnectorMessage.CapStatus cstatus = null;
            switch (connectorMessage.getStatus()) {
            case ERROR:
                cstatus = ERROR;
                break;
            case FILTERED:
                cstatus = FILTERED;
                break;
            case PENDING:
                cstatus = PENDING;
                break;
            case QUEUED:
                cstatus = QUEUED;
                break;
            case RECEIVED:
                cstatus = RECEIVED;
                break;
            case SENT:
                cstatus = CapStatus.SENT;
                break;
            case TRANSFORMED:
                cstatus = CapStatus.TRANSFORMED;
                break;
            }
            cb.setStatus(cstatus);

            CapnpOutputStream out = new CapnpOutputStream(bufAlloc.buffer());
            SerializePacked.write(out, rmb.getMb());
            long localchannelId = getLocalChannelId(connectorMessage.getChannelId());
            DatabaseEntry key = new DatabaseEntry(buildPrimaryKeyOfConnectorMessage(connectorMessage.getMessageId(), connectorMessage.getMetaDataId()));
            DatabaseEntry data = out.getData();
            
            dbMap.get("d_mm" + localchannelId).put(txn, key, data);
            if (storeMaps) {
                updateSourceMap(connectorMessage);
                updateMaps(connectorMessage);
            }

            updateErrors(connectorMessage);

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

        PreparedStatement statement = null;
        try {
            cascadeMessageDelete("deleteMessageCascadeAttachments", messageId, channelId);
            cascadeMessageDelete("deleteMessageCascadeMetadata", messageId, channelId);
            cascadeMessageDelete("deleteMessageCascadeContent", messageId, channelId);
            cascadeMessageDelete("deleteMessageCascadeConnectorMessage", messageId, channelId);

            statement = prepareStatement("deleteMessage", channelId);
            statement.setLong(1, messageId);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            closeDatabaseObjectIfNeeded(statement);
        }
    }

    @Override
    public void deleteConnectorMessages(String channelId, long messageId, Set<Integer> metaDataIds) {
        logger.debug(channelId + "/" + messageId + ": deleting connector messages");
        long localChannelId = getLocalChannelId(channelId);

        PreparedStatement statement = null;
        try {
            if (metaDataIds == null) {
                cascadeMessageDelete("deleteMessageCascadeMetadata", messageId, channelId);
                cascadeMessageDelete("deleteMessageCascadeContent", messageId, channelId);

                statement = prepareStatement("deleteConnectorMessages", channelId);
                statement.setLong(1, messageId);
                statement.executeUpdate();
            } else {
                Map<String, Object> values = new HashMap<String, Object>();
                values.put("localChannelId", localChannelId);
                values.put("metaDataIds", StringUtils.join(metaDataIds, ','));

                cascadeMessageDelete("deleteConnectorMessagesByMetaDataIdsCascadeContent", messageId, values);
                cascadeMessageDelete("deleteConnectorMessagesByMetaDataIdsCascadeMetadata", messageId, values);

                statement = null;

                try {
                    statement = getConnection().prepareStatement(querySource.getQuery("deleteConnectorMessagesByMetaDataIds", values));
                    statement.setLong(1, messageId);
                    statement.executeUpdate();
                } finally {
                    close(statement);
                }
            }
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            closeDatabaseObjectIfNeeded(statement);
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

        PreparedStatement statement = null;
        try {
            // don't decrement the previous status if it was RECEIVED
            if (previousStatus == Status.RECEIVED) {
                previousStatus = null;
            }

            transactionStats.update(connectorMessage.getChannelId(), connectorMessage.getMetaDataId(), connectorMessage.getStatus(), previousStatus);

            statement = prepareStatement("updateStatus", connectorMessage.getChannelId());
            statement.setString(1, Character.toString(connectorMessage.getStatus().getStatusCode()));
            statement.setInt(2, connectorMessage.getMetaDataId());
            statement.setLong(3, connectorMessage.getMessageId());
            statement.setString(4, connectorMessage.getServerId());

            if (statement.executeUpdate() == 0) {
                throw new DonkeyDaoException("Failed to update connector message status, the connector message was removed from this server.");
            }
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            closeDatabaseObjectIfNeeded(statement);
        }
    }

    @Override
    public void updateErrors(ConnectorMessage connectorMessage) {
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

        if (errorsUpdated) {
            updateErrorCode(connectorMessage);
        }
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
        PreparedStatement statement = null;
        try {
            statement = prepareStatement("updateErrorCode", connectorMessage.getChannelId());

            statement.setInt(1, connectorMessage.getErrorCode());
            statement.setInt(2, connectorMessage.getMetaDataId());
            statement.setLong(3, connectorMessage.getMessageId());
            statement.setString(4, connectorMessage.getServerId());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            closeDatabaseObjectIfNeeded(statement);
        }
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
        try {
            long localChannelId = getLocalChannelId(channelId);
            DatabaseEntry key = new DatabaseEntry(longToBytes(messageId));
            DatabaseEntry data = new DatabaseEntry();
            Database messageDb = dbMap.get("d_m" + localChannelId);
            OperationStatus os = messageDb.get(txn, key, data, LockMode.READ_COMMITTED);
            if(os == OperationStatus.SUCCESS) {
                CapnpInputStream ai = new CapnpInputStream(data.getData());
                MessageReader mr = SerializePacked.read(ai);
                CapMessage.Reader atReader= mr.getRoot(CapMessage.factory);

                ReusableMessageBuilder rmb = objectPool.borrowObject(CapMessage.class);
                rmb.getMb().setRoot(CapMessage.factory, atReader);
                CapMessage.Builder mb = (CapMessage.Builder)rmb.getSb();
                mb.setProcessed(true);

                if(reset) {
                    mb.setImportChannelId((String)null);
                    mb.setImportId(0);
                }

                CapnpOutputStream out = new CapnpOutputStream(bufAlloc.buffer());
                SerializePacked.write(out, rmb.getMb());
                data = out.getData();
                messageDb.put(txn, key, data);
                objectPool.returnObject(CapMessage.class, rmb);
            }
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
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
                cursor.close();
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
            dbNames.add("d_mc" + localChannelId);
            dbNames.add("d_mm" + localChannelId);
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
        Cursor cursor = dbMap.get(TABLE_D_CHANNELS).openCursor(txn, null);
        try {
            long maxLocalChannelId = 0;
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry val = new DatabaseEntry();
            while(cursor.getNext(key, val, LockMode.READ_COMMITTED) == OperationStatus.SUCCESS) {
                long tmp = bytesToLong(key.getData());
                if(tmp > maxLocalChannelId) {
                    maxLocalChannelId = tmp;
                }
            }
            
            return maxLocalChannelId;
        } catch (DatabaseException e) {
            throw new DonkeyDaoException(e);
        } finally {
            cursor.close();
        }
    }

    @Override
    public void deleteAllMessages(String channelId) {
        logger.debug(channelId + ": deleting all messages");

        List<AutoCloseable> statements = new ArrayList<>();
        try {
            // delete tables without constraints
            cascadeMessageDelete("deleteAllMessagesCascadeAttachments", channelId);
            cascadeMessageDelete("deleteAllMessagesCascadeMetadata", channelId);
            cascadeMessageDelete("deleteAllMessagesCascadeContent", channelId);

            PreparedStatement statement = null;
            // remove constraints so truncate can execute
            if (querySource.queryExists("dropConstraintMessageContentTable")) {
                statement = prepareStatement("dropConstraintMessageContentTable", channelId);
                statements.add(statement);
                statement.executeUpdate();
            }
            if (querySource.queryExists("dropConstraintMessageCustomMetaDataTable")) {
                statement = prepareStatement("dropConstraintMessageCustomMetaDataTable", channelId);
                statements.add(statement);
                statement.executeUpdate();
            }

            // delete
            cascadeMessageDelete("deleteAllMessagesCascadeConnectorMessage", channelId);

            // re-add constraints
            if (querySource.queryExists("addConstraintMessageCustomMetaDataTable")) {
                statement = prepareStatement("addConstraintMessageCustomMetaDataTable", channelId);
                statements.add(statement);
                statement.executeUpdate();
            }
            if (querySource.queryExists("addConstraintMessageContentTable")) {
                statement = prepareStatement("addConstraintMessageContentTable", channelId);
                statements.add(statement);
                statement.executeUpdate();
            }

            // remove constraints so truncate can execute
            if (querySource.queryExists("dropConstraintConnectorMessageTable")) {
                statement = prepareStatement("dropConstraintConnectorMessageTable", channelId);
                statements.add(statement);
                statement.executeUpdate();
            }
            if (querySource.queryExists("dropConstraintAttachmentTable")) {
                statement = prepareStatement("dropConstraintAttachmentTable", channelId);
                statements.add(statement);
                statement.executeUpdate();
            }

            // delete
            statement = prepareStatement("deleteAllMessages", channelId);
            statements.add(statement);
            statement.executeUpdate();

            // re-add constraints
            if (querySource.queryExists("addConstraintAttachmentTable")) {
                statement = prepareStatement("addConstraintAttachmentTable", channelId);
                statements.add(statement);
                statement.executeUpdate();
            }
            if (querySource.queryExists("addConstraintConnectorMessageTable")) {
                statement = prepareStatement("addConstraintConnectorMessageTable", channelId);
                statements.add(statement);
                statement.executeUpdate();
            }

        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            closeDatabaseObjectsIfNeeded(statements);
        }
    }

    @Override
    public void deleteMessageContent(String channelId, long messageId) {
        logger.debug(channelId + "/" + messageId + ": deleting content");

        PreparedStatement statement = null;
        try {
            statement = prepareStatement("deleteMessageContent", channelId);
            statement.setLong(1, messageId);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            closeDatabaseObjectIfNeeded(statement);
        }
    }

    @Override
    public void deleteMessageContentByMetaDataIds(String channelId, long messageId, Set<Integer> metaDataIds) {
        logger.debug(channelId + "/" + messageId + ": deleting content by metadata IDs: " + String.valueOf(metaDataIds));

        PreparedStatement statement = null;
        try {
            Map<String, Object> values = new HashMap<String, Object>();
            values.put("localChannelId", getLocalChannelId(channelId));
            values.put("metaDataIds", StringUtils.join(metaDataIds, ','));

            statement = getConnection().prepareStatement(querySource.getQuery("deleteMessageContentByMetaDataIds", values));
            statement.setLong(1, messageId);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            closeDatabaseObjectIfNeeded(statement);
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

        PreparedStatement statement = null;
        try {
            statement = prepareStatement("deleteMessageAttachments", channelId);
            statement.setLong(1, messageId);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            closeDatabaseObjectIfNeeded(statement);
        }
    }

    @Override
    public List<MetaDataColumn> getMetaDataColumns(String channelId) {
        ResultSet columns = null;

        try {
            List<MetaDataColumn> metaDataColumns = new ArrayList<MetaDataColumn>();
            long localChannelId = getLocalChannelId(channelId);

            columns = connection.getMetaData().getColumns(connection.getCatalog(), null, "d_mcm" + localChannelId, null);

            if (!columns.next()) {
                columns.close();
                columns = connection.getMetaData().getColumns(connection.getCatalog(), null, "D_MCM" + localChannelId, null);

                if (!columns.next()) {
                    return metaDataColumns;
                }
            }

            do {
                String name = columns.getString("COLUMN_NAME");

                if (!name.equalsIgnoreCase("METADATA_ID") && !name.equalsIgnoreCase("MESSAGE_ID")) {
                    MetaDataColumnType columnType = MetaDataColumnType.fromSqlType(columns.getInt("DATA_TYPE"));

                    if (columnType == null) {
                        logger.error("Invalid custom metadata column: " + name + " (type: " + sqlTypeToString(columns.getInt("DATA_TYPE")) + ").");
                    } else {
                        metaDataColumns.add(new MetaDataColumn(name, columnType, null));
                    }
                }
            } while (columns.next());

            return metaDataColumns;
        } catch (Exception e) {
            throw new DonkeyDaoException("Failed to retrieve meta data columns", e);
        } finally {
            close(columns);
        }
    }

    @Override
    public void removeMetaDataColumn(String channelId, String columnName) {
        logger.debug(channelId + "/" + ": removing custom meta data column (" + columnName + ")");
        Statement statement = null;

        try {
            Map<String, Object> values = new HashMap<String, Object>();
            values.put("localChannelId", getLocalChannelId(channelId));
            values.put("columnName", columnName);

            statement = connection.createStatement();

            if (querySource.queryExists("removeMetaDataColumnIndex")) {
                statement.executeUpdate(querySource.getQuery("removeMetaDataColumnIndex", values));
            }

            statement.executeUpdate(querySource.getQuery("removeMetaDataColumn", values));
        } catch (SQLException e) {
            throw new DonkeyDaoException("Failed to remove meta-data column", e);
        } finally {
            close(statement);
        }
    }

    @Override
    public long getMaxMessageId(String channelId) {
        ResultSet resultSet = null;
        PreparedStatement statement = null;

        try {
            statement = prepareStatement("getMaxMessageId", channelId);
            resultSet = statement.executeQuery();
            return (resultSet.next()) ? resultSet.getLong(1) : null;
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            close(resultSet);
            closeDatabaseObjectIfNeeded(statement);
        }
    }

    @Override
    public long getMinMessageId(String channelId) {
        ResultSet resultSet = null;
        PreparedStatement statement = null;

        try {
            statement = prepareStatement("getMinMessageId", channelId);
            resultSet = statement.executeQuery();
            return (resultSet.next()) ? resultSet.getLong(1) : null;
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            close(resultSet);
            closeDatabaseObjectIfNeeded(statement);
        }
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
        ResultSet resultSet = null;
        PreparedStatement statement = null;

        try {
            // Get the total size of each attachment by summing the sizes of its segments
            statement = prepareStatement("selectMessageAttachmentSizeByMessageId", channelId);
            statement.setLong(1, messageId);
            resultSet = statement.executeQuery();

            Map<String, Integer> attachmentSize = new HashMap<String, Integer>();
            while (resultSet.next()) {
                // Store the attachment size in a map with the attachment id as the key
                attachmentSize.put(resultSet.getString("id"), resultSet.getInt("attachment_size"));
            }

            close(resultSet);
            closeDatabaseObjectIfNeeded(statement);

            // Get the attachment data
            statement = prepareStatement("selectMessageAttachmentByMessageId", channelId);
            statement.setLong(1, messageId);
            // Set the number of rows to be fetched into memory at a time. This limits the amount of memory required for the query.
            statement.setFetchSize(1);
            resultSet = statement.executeQuery();

            // Initialize the return object
            List<Attachment> attachments = new ArrayList<Attachment>();
            // The current attachment id that is being stitched together
            String currentAttachmentId = null;
            // The type of the current attachment
            String type = null;
            // Use an byte array to combine the segments
            byte[] content = null;
            int offset = 0;

            while (resultSet.next()) {
                // Get the attachment id of the current segment
                String attachmentId = resultSet.getString("id");

                // Ensure that the attachmentId is in the map we created earlier, otherwise don't return this attachment
                if (attachmentSize.containsKey(attachmentId)) {
                    // If starting a new attachment
                    if (!attachmentId.equals(currentAttachmentId)) {
                        // If there was a previous attachment, we need to finish it.
                        if (content != null) {
                            // Add the data in the output stream to the list of attachments to return
                            attachments.add(new Attachment(currentAttachmentId, content, type));
                        }
                        currentAttachmentId = attachmentId;
                        type = resultSet.getString("type");

                        // Initialize the byte array size to the exact size of the attachment. This should minimize the memory requirements if the numbers are correct.
                        // Use 0 as a backup in case the size is not in the map. (If trying to return an attachment that no longer exists)
                        content = new byte[attachmentSize.get(attachmentId)];
                        offset = 0;
                    }

                    // write the current segment to the output stream buffer
                    byte[] segment = resultSet.getBytes("content");
                    System.arraycopy(segment, 0, content, offset, segment.length);

                    offset += segment.length;
                }
            }

            // Finish the message if one exists by adding it to the list of attachments to return
            if (content != null) {
                attachments.add(new Attachment(currentAttachmentId, content, type));
            }
            content = null;

            return attachments;
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            close(resultSet);
            closeDatabaseObjectIfNeeded(statement);
        }
    }

    @Override
    public Attachment getMessageAttachment(String channelId, String attachmentId, Long messageId) {
        ResultSet resultSet = null;
        Attachment attachment = new Attachment();
        PreparedStatement statement = null;
        try {
            // Get the total size of each attachment by summing the sizes of its segments
            statement = prepareStatement("selectMessageAttachmentSize", channelId);
            statement.setString(1, attachmentId);
            statement.setLong(2, messageId);
            resultSet = statement.executeQuery();

            int size = 0;
            if (resultSet.next()) {
                // Store the attachment size in a map with the attachment id as the key
                size = resultSet.getInt("attachment_size");
            }

            close(resultSet);
            closeDatabaseObjectIfNeeded(statement);

            // Get the attachment data
            statement = prepareStatement("selectMessageAttachment", channelId);
            statement.setString(1, attachmentId);
            statement.setLong(2, messageId);
            // Set the number of rows to be fetched into memory at a time. This limits the amount of memory required for the query.
            statement.setFetchSize(1);
            resultSet = statement.executeQuery();

            // The type of the current attachment
            String type = null;

            // Initialize the output stream's buffer size to the exact size of the attachment. This should minimize the memory requirements if the numbers are correct.
            byte[] content = null;
            int offset = 0;

            while (resultSet.next()) {
                if (content == null) {
                    type = resultSet.getString("type");
                    content = new byte[size];
                }

                // write the current segment to the output stream buffer
                byte[] segment = resultSet.getBytes("content");
                System.arraycopy(segment, 0, content, offset, segment.length);

                offset += segment.length;
            }

            // Finish the message if one exists by adding it to the list of attachments to return
            if (content != null) {
                attachment.setId(attachmentId);
                attachment.setContent(content);
                attachment.setType(type);
            }
            content = null;

            return attachment;
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            close(resultSet);
            closeDatabaseObjectIfNeeded(statement);
        }
    }

    @Override
    public List<Message> getUnfinishedMessages(String channelId, String serverId, int limit, Long minMessageId) {
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        try {
            Map<Long, Message> messageMap = new HashMap<Long, Message>();
            List<Message> messageList = new ArrayList<Message>();
            List<Long> messageIds = new ArrayList<Long>();

            Map<String, Object> params = new HashMap<String, Object>();
            params.put("localChannelId", getLocalChannelId(channelId));
            params.put("limit", limit);

            statement = connection.prepareStatement(querySource.getQuery("getUnfinishedMessages", params));
            statement.setLong(1, minMessageId);
            statement.setString(2, serverId);
            resultSet = statement.executeQuery();

            while (resultSet.next()) {
                Message message = getMessageFromResultSet(channelId, resultSet);
                messageMap.put(message.getMessageId(), message);
                messageList.add(message);
                messageIds.add(message.getMessageId());
            }

            close(resultSet);
            close(statement);

            if (!messageIds.isEmpty()) {
                params = new HashMap<String, Object>();
                params.put("localChannelId", getLocalChannelId(channelId));
                params.put("messageIds", StringUtils.join(messageIds, ","));

                statement = connection.prepareStatement(querySource.getQuery("getConnectorMessagesByMessageIds", params));
                resultSet = statement.executeQuery();

                while (resultSet.next()) {
                    ConnectorMessage connectorMessage = getConnectorMessageFromResultSet(channelId, resultSet, true, true);
                    messageMap.get(connectorMessage.getMessageId()).getConnectorMessages().put(connectorMessage.getMetaDataId(), connectorMessage);
                }
            }

            return messageList;
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            close(resultSet);
            close(statement);
        }
    }

    @Override
    public List<Message> getPendingConnectorMessages(String channelId, String serverId, int limit, Long minMessageId) {
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        try {
            Map<Long, Message> messageMap = new HashMap<Long, Message>();
            List<Message> messageList = new ArrayList<Message>();
            List<Long> messageIds = new ArrayList<Long>();

            Map<String, Object> params = new HashMap<String, Object>();
            params.put("localChannelId", getLocalChannelId(channelId));
            params.put("limit", limit);

            statement = connection.prepareStatement(querySource.getQuery("getPendingMessageIds", params));
            statement.setLong(1, minMessageId);
            statement.setString(2, serverId);

            resultSet = statement.executeQuery();

            while (resultSet.next()) {
                Message message = new Message();
                message.setMessageId(resultSet.getLong(1));
                messageMap.put(message.getMessageId(), message);
                messageList.add(message);
                messageIds.add(message.getMessageId());
            }

            close(resultSet);
            close(statement);

            if (!messageIds.isEmpty()) {
                params = new HashMap<String, Object>();
                params.put("localChannelId", getLocalChannelId(channelId));
                params.put("messageIds", StringUtils.join(messageIds, ","));

                statement = connection.prepareStatement(querySource.getQuery("getPendingConnectorMessages", params));
                statement.setString(1, serverId);

                resultSet = statement.executeQuery();

                while (resultSet.next()) {
                    ConnectorMessage connectorMessage = getConnectorMessageFromResultSet(channelId, resultSet, true, true);
                    messageMap.get(connectorMessage.getMessageId()).getConnectorMessages().put(connectorMessage.getMetaDataId(), connectorMessage);
                }
            }

            return messageList;
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            close(resultSet);
            close(statement);
        }
    }

    @Override
    public List<Message> getMessages(String channelId, List<Long> messageIds) {
        if (messageIds.size() > 1000) {
            throw new DonkeyDaoException("Only up to 1000 message Ids at a time are supported.");
        }

        PreparedStatement statement = null;
        ResultSet resultSet = null;
        Map<Long, Message> messageMap = new LinkedHashMap<Long, Message>();

        try {
            Map<String, Object> params = new HashMap<String, Object>();
            params.put("localChannelId", getLocalChannelId(channelId));
            params.put("messageIds", StringUtils.join(messageIds, ","));

            statement = connection.prepareStatement(querySource.getQuery("getMessagesByMessageIds", params));
            resultSet = statement.executeQuery();

            // Get all message objects in the list and store them so they are accessible by message Id
            while (resultSet.next()) {
                Message message = getMessageFromResultSet(channelId, resultSet);
                messageMap.put(message.getMessageId(), message);
            }

            if (!messageIds.isEmpty()) {
                close(resultSet);
                close(statement);

                // Cache all message content for all messages in the list
                Map<Long, Map<Integer, List<MessageContent>>> messageContentMap = getMessageContent(channelId, messageIds);
                // Cache all metadata maps for all messages in the list
                Map<Long, Map<Integer, Map<String, Object>>> metaDataMaps = getMetaDataMaps(channelId, messageIds);

                params = new HashMap<String, Object>();
                params.put("localChannelId", getLocalChannelId(channelId));
                params.put("messageIds", StringUtils.join(messageIds, ","));

                // Perform a single query to retrieve all connector messages in the list at once
                statement = connection.prepareStatement(querySource.getQuery("getConnectorMessagesByMessageIds", params));
                resultSet = statement.executeQuery();

                ConnectorMessage sourceConnectorMessage = null;
                long currentMessageId = 0;
                while (resultSet.next()) {
                    // Create the connector from the result set without content or metadata map
                    ConnectorMessage connectorMessage = getConnectorMessageFromResultSet(channelId, resultSet, false, false);
                    // Store the reference to the connector in the message
                    messageMap.get(connectorMessage.getMessageId()).getConnectorMessages().put(connectorMessage.getMetaDataId(), connectorMessage);

                    // Whenever we start a new message, null the sourceConnectorMessage. This is in case the message has no source connector so we don't use the previous message's source
                    if (currentMessageId != connectorMessage.getMessageId()) {
                        currentMessageId = connectorMessage.getMessageId();
                        sourceConnectorMessage = null;
                    }

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
                }
            }

            return new ArrayList<Message>(messageMap.values());
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            close(resultSet);
            close(statement);
        }
    }

    @Override
    public List<ConnectorMessage> getConnectorMessages(String channelId, String serverId, int metaDataId, Status status, int offset, int limit, Long minMessageId, Long maxMessageId) {
        List<ConnectorMessage> connectorMessages = new ArrayList<ConnectorMessage>();

        if (limit == 0) {
            return connectorMessages;
        }

        PreparedStatement statement = null;
        ResultSet resultSet = null;

        try {
            Map<String, Object> params = new HashMap<String, Object>();
            params.put("localChannelId", getLocalChannelId(channelId));
            params.put("offset", offset);
            params.put("limit", limit);

            int index = 1;

            if (minMessageId == null || maxMessageId == null) {
                statement = connection.prepareStatement(querySource.getQuery(serverId != null ? "getConnectorMessagesByMetaDataIdAndStatusWithLimit" : "getConnectorMessagesByMetaDataIdAndStatusWithLimitAllServers", params));
                statement.setInt(index++, metaDataId);
                statement.setString(index++, Character.toString(status.getStatusCode()));
                if (serverId != null) {
                    statement.setString(index++, serverId);
                }
            } else {
                statement = connection.prepareStatement(querySource.getQuery(serverId != null ? "getConnectorMessagesByMetaDataIdAndStatusWithLimitAndRange" : "getConnectorMessagesByMetaDataIdAndStatusWithLimitAndRangeAllServers", params));
                statement.setInt(index++, metaDataId);
                statement.setString(index++, Character.toString(status.getStatusCode()));
                if (serverId != null) {
                    statement.setString(index++, serverId);
                }
                statement.setLong(index++, minMessageId);
                statement.setLong(index++, maxMessageId);
            }

            resultSet = statement.executeQuery();

            while (resultSet.next()) {
                connectorMessages.add(getConnectorMessageFromResultSet(channelId, resultSet, true, true));
            }

            return connectorMessages;
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            close(resultSet);
            close(statement);
        }
    }

    @Override
    public List<ConnectorMessage> getConnectorMessages(String channelId, long messageId, Set<Integer> metaDataIds, boolean includeContent) {
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        try {
            Map<String, Object> values = new HashMap<String, Object>();
            values.put("localChannelId", getLocalChannelId(channelId));
            values.put("metaDataIds", StringUtils.join(metaDataIds, ','));

            statement = connection.prepareStatement(querySource.getQuery("getConnectorMessagesByMessageIdAndMetaDataIds", values));
            statement.setLong(1, messageId);
            resultSet = statement.executeQuery();

            List<ConnectorMessage> connectorMessages = new ArrayList<ConnectorMessage>();

            while (resultSet.next()) {
                connectorMessages.add(getConnectorMessageFromResultSet(channelId, resultSet, includeContent, true));
            }

            return connectorMessages;
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            close(resultSet);
            close(statement);
        }
    }

    @Override
    public Map<Integer, ConnectorMessage> getConnectorMessages(String channelId, long messageId, List<Integer> metaDataIds) {
        ResultSet resultSet = null;
        PreparedStatement statement = null;

        try {
            boolean includeMetaDataIds = CollectionUtils.isNotEmpty(metaDataIds);
            Map<String, Object> params = new HashMap<String, Object>();
            params.put("localChannelId", getLocalChannelId(channelId));

            if (includeMetaDataIds) {
                params.put("metaDataIds", StringUtils.join(metaDataIds, ','));
            }

            statement = getConnection().prepareStatement(querySource.getQuery(includeMetaDataIds ? "getConnectorMessagesByMessageIdAndMetaDataIds" : "getConnectorMessagesByMessageId", params));
            statement.setLong(1, messageId);
            resultSet = statement.executeQuery();

            Map<Integer, ConnectorMessage> connectorMessages = new HashMap<Integer, ConnectorMessage>();

            while (resultSet.next()) {
                ConnectorMessage connectorMessage = getConnectorMessageFromResultSet(channelId, resultSet, true, true);
                connectorMessages.put(connectorMessage.getMetaDataId(), connectorMessage);
            }

            return connectorMessages;
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            close(resultSet);
            closeDatabaseObjectIfNeeded(statement);
        }
    }

    @Override
    public Map<Integer, Status> getConnectorMessageStatuses(String channelId, long messageId, boolean checkProcessed) {
        ResultSet resultSet = null;
        PreparedStatement statement = null;

        try {
            statement = prepareStatement(checkProcessed ? "getConnectorMessageStatusesCheckProcessed" : "getConnectorMessageStatuses", channelId);
            statement.setLong(1, messageId);
            resultSet = statement.executeQuery();

            Map<Integer, Status> statusMap = new HashMap<Integer, Status>();

            while (resultSet.next()) {
                statusMap.put(resultSet.getInt(1), Status.fromChar(resultSet.getString(2).charAt(0)));
            }

            return statusMap;
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            close(resultSet);
            closeDatabaseObjectIfNeeded(statement);
        }
    }

    @Override
    public int getConnectorMessageCount(String channelId, String serverId, int metaDataId, Status status) {
        if (donkey.getDeployedChannels().get(channelId) != null || getLocalChannelIds().get(channelId) != null) {
            ResultSet resultSet = null;
            PreparedStatement statement = null;

            try {
                statement = statementSource.getPreparedStatement(serverId != null ? "getConnectorMessageCountByMetaDataIdAndStatus" : "getConnectorMessageCountByMetaDataIdAndStatusAllServers", getLocalChannelId(channelId));
                statement.setInt(1, metaDataId);
                statement.setString(2, Character.toString(status.getStatusCode()));
                if (serverId != null) {
                    statement.setString(3, serverId);
                }
                resultSet = statement.executeQuery();
                resultSet.next();
                return resultSet.getInt(1);
            } catch (SQLException e) {
                throw new DonkeyDaoException(e);
            } finally {
                close(resultSet);
                closeDatabaseObjectIfNeeded(statement);
            }
        } else {
            // the channel has never been deployed
            return 0;
        }
    }

    @Override
    public long getConnectorMessageMaxMessageId(String channelId, String serverId, int metaDataId, Status status) {
        ResultSet resultSet = null;
        PreparedStatement statement = null;

        try {
            statement = prepareStatement(serverId != null ? "getConnectorMessageMaxMessageIdByMetaDataIdAndStatus" : "getConnectorMessageMaxMessageIdByMetaDataIdAndStatusAllServers", channelId);
            statement.setInt(1, metaDataId);
            statement.setString(2, Character.toString(status.getStatusCode()));
            if (serverId != null) {
                statement.setString(3, serverId);
            }
            resultSet = statement.executeQuery();
            resultSet.next();
            return resultSet.getLong(1);
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            close(resultSet);
            closeDatabaseObjectIfNeeded(statement);
        }
    }

    @Override
    public void addMetaDataColumn(String channelId, MetaDataColumn metaDataColumn) {
        logger.debug(channelId + ": adding custom meta data column (" + metaDataColumn.getName() + ")");
        Statement statement = null;

        try {
            Map<String, Object> values = new HashMap<String, Object>();
            values.put("localChannelId", getLocalChannelId(channelId));
            values.put("columnName", metaDataColumn.getName());

            String queryName = "addMetaDataColumn" + StringUtils.capitalize(StringUtils.lowerCase(metaDataColumn.getType().toString()));

            statement = connection.createStatement();
            statement.executeUpdate(querySource.getQuery(queryName, values));

            if (querySource.queryExists(queryName + "Index")) {
                statement.executeUpdate(querySource.getQuery(queryName + "Index", values));
            }
        } catch (SQLException e) {
            throw new DonkeyDaoException("Failed to add meta-data column", e);
        } finally {
            close(statement);
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
        PreparedStatement statement = null;

        try {
            if (statuses == null || statuses.size() == 0) {
                return;
            }

            Map<String, Object> values = new HashMap<String, Object>();
            values.put("localChannelId", getLocalChannelId(channelId));

            int count = 0;
            StringBuilder builder = new StringBuilder();
            for (Status status : statuses) {
                count++;

                if (count > 1) {
                    builder.append(",");
                }
                builder.append(status.toString() + " = 0");
            }
            values.put("statuses", builder.toString());

            String queryName = (metaDataId == null) ? "resetChannelStatistics" : "resetConnectorStatistics";
            statement = connection.prepareStatement(querySource.getQuery(queryName, values));
            statement.setString(1, statsServerId);

            if (metaDataId != null) {
                statement.setInt(2, metaDataId);
            }

            statement.executeUpdate();

            if (!resetCurrentStats.containsKey(channelId)) {
                resetCurrentStats.put(channelId, new HashMap<Integer, Set<Status>>());
            }

            Map<Integer, Set<Status>> metaDataIds = resetCurrentStats.get(channelId);

            if (!metaDataIds.containsKey(metaDataId)) {
                metaDataIds.put(metaDataId, statuses);
            }

        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            close(statement);
        }
    }

    @Override
    public void resetAllStatistics(String channelId) {
        logger.debug(channelId + ": resetting all statistics (including lifetime)");
        PreparedStatement statement = null;

        try {
            Map<String, Object> values = new HashMap<String, Object>();
            values.put("localChannelId", getLocalChannelId(channelId));

            statement = connection.prepareStatement(querySource.getQuery("resetAllStatistics", values));
            statement.setString(1, statsServerId);
            statement.executeUpdate();

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
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            close(statement);
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
        String queryId = (total) ? "getChannelTotalStatistics" : "getChannelStatistics";
        Statistics statistics = new Statistics(!total);
        ResultSet resultSet = null;

        for (String channelId : channelIds.keySet()) {
            PreparedStatement statement = null;
            try {
                statement = prepareStatement(queryId, channelId);
                statement.setString(1, serverId);
                resultSet = statement.executeQuery();

                while (resultSet.next()) {
                    Integer metaDataId = resultSet.getInt("metadata_id");

                    if (resultSet.wasNull()) {
                        metaDataId = null;
                    }

                    Map<Status, Long> stats = new HashMap<Status, Long>();
                    stats.put(Status.RECEIVED, resultSet.getLong("received"));
                    stats.put(Status.FILTERED, resultSet.getLong("filtered"));
                    stats.put(Status.SENT, resultSet.getLong("sent"));
                    stats.put(Status.ERROR, resultSet.getLong("error"));

                    statistics.overwrite(channelId, metaDataId, stats);
                }
            } catch (SQLException e) {
                throw new DonkeyDaoException(e);
            } finally {
                close(resultSet);
                closeDatabaseObjectIfNeeded(statement);
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
        logger.debug("Committing transaction" + (durable ? "" : " asynchronously"));

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

    public Connection getConnection() {
        return connection;
    }

    public String getAsyncCommitCommand() {
        return asyncCommitCommand;
    }

    public void setAsyncCommitCommand(String asyncCommitCommand) {
        this.asyncCommitCommand = asyncCommitCommand;
    }

    private Message getMessageFromResultSet(String channelId, ResultSet resultSet) {
        try {
            Message message = new Message();
            long messageId = resultSet.getLong("id");
            Calendar receivedDate = Calendar.getInstance();
            receivedDate.setTimeInMillis(resultSet.getTimestamp("received_date").getTime());

            message.setMessageId(messageId);
            message.setChannelId(channelId);
            message.setReceivedDate(receivedDate);
            message.setProcessed(resultSet.getBoolean("processed"));
            message.setServerId(resultSet.getString("server_id"));
            message.setImportChannelId(resultSet.getString("import_channel_id"));

            long importId = resultSet.getLong("import_id");
            if (!resultSet.wasNull()) {
                message.setImportId(importId);
            }

            long originalId = resultSet.getLong("original_id");
            if (!resultSet.wasNull()) {
                message.setOriginalId(originalId);
            }

            return message;
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        }
    }

    private ConnectorMessage getConnectorMessageFromResultSet(String channelId, ResultSet resultSet, boolean includeContent, boolean includeMetaDataMap) {
        try {
            ConnectorMessage connectorMessage = new ConnectorMessage();
            long messageId = resultSet.getLong("message_id");
            int metaDataId = resultSet.getInt("id");
            Calendar receivedDate = Calendar.getInstance();
            receivedDate.setTimeInMillis(resultSet.getTimestamp("received_date").getTime());

            Calendar sendDate = null;
            Timestamp sendDateTimestamp = resultSet.getTimestamp("send_date");
            if (sendDateTimestamp != null) {
                sendDate = Calendar.getInstance();
                sendDate.setTimeInMillis(sendDateTimestamp.getTime());
            }

            Calendar responseDate = null;
            Timestamp responseDateTimestamp = resultSet.getTimestamp("response_date");
            if (responseDateTimestamp != null) {
                responseDate = Calendar.getInstance();
                responseDate.setTimeInMillis(responseDateTimestamp.getTime());
            }

            connectorMessage.setChannelName(getDeployedChannelName(channelId));
            connectorMessage.setMessageId(messageId);
            connectorMessage.setMetaDataId(metaDataId);
            connectorMessage.setChannelId(channelId);
            connectorMessage.setServerId(resultSet.getString("server_id"));
            connectorMessage.setConnectorName(resultSet.getString("connector_name"));
            connectorMessage.setReceivedDate(receivedDate);
            connectorMessage.setStatus(Status.fromChar(resultSet.getString("status").charAt(0)));
            connectorMessage.setSendAttempts(resultSet.getInt("send_attempts"));
            connectorMessage.setSendDate(sendDate);
            connectorMessage.setResponseDate(responseDate);
            connectorMessage.setErrorCode(resultSet.getInt("error_code"));
            connectorMessage.setChainId(resultSet.getInt("chain_id"));
            connectorMessage.setOrderId(resultSet.getInt("order_id"));

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
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        }
    }

    /**
     * Get all message content for a messageId and metaDataId
     */
    private List<MessageContent> getMessageContent(String channelId, long messageId, int metaDataId) {
        List<MessageContent> messageContents = new ArrayList<MessageContent>();
        ResultSet resultSet = null;

        PreparedStatement statement = null;
        try {
            statement = prepareStatement("getMessageContent", channelId);
            statement.setLong(1, messageId);
            statement.setInt(2, metaDataId);

            resultSet = statement.executeQuery();

            while (resultSet.next()) {
                String content = resultSet.getString("content");
                ContentType contentType = ContentType.fromCode(resultSet.getInt("content_type"));
                String dataType = resultSet.getString("data_type");
                boolean encrypted = resultSet.getBoolean("is_encrypted");

                if ((decryptData || alwaysDecrypt.contains(contentType)) && encrypted && encryptor != null) {
                    content = encryptor.decrypt(content);
                    encrypted = false;
                }

                messageContents.add(new MessageContent(channelId, messageId, metaDataId, contentType, content, dataType, encrypted));
            }
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            close(resultSet);
            closeDatabaseObjectIfNeeded(statement);
        }

        return messageContents;
    }

    private Map<Long, Map<Integer, List<MessageContent>>> getMessageContent(String channelId, List<Long> messageIds) {
        if (messageIds.size() > 1000) {
            throw new DonkeyDaoException("Only up to 1000 message Ids at a time are supported.");
        }

        Map<Long, Map<Integer, List<MessageContent>>> messageContentMap = new LinkedHashMap<Long, Map<Integer, List<MessageContent>>>();
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        try {
            Map<String, Object> params = new HashMap<String, Object>();
            params.put("localChannelId", getLocalChannelId(channelId));
            params.put("messageIds", StringUtils.join(messageIds, ","));

            statement = connection.prepareStatement(querySource.getQuery("getMessageContentByMessageIds", params));

            resultSet = statement.executeQuery();

            while (resultSet.next()) {
                Long messageId = resultSet.getLong("message_id");
                Integer metaDataId = resultSet.getInt("metadata_id");
                String content = resultSet.getString("content");
                ContentType contentType = ContentType.fromCode(resultSet.getInt("content_type"));
                String dataType = resultSet.getString("data_type");
                boolean encrypted = resultSet.getBoolean("is_encrypted");

                if ((decryptData || alwaysDecrypt.contains(contentType)) && encrypted && encryptor != null) {
                    content = encryptor.decrypt(content);
                    encrypted = false;
                }

                Map<Integer, List<MessageContent>> connectorMessageContentMap = messageContentMap.get(messageId);
                if (connectorMessageContentMap == null) {
                    connectorMessageContentMap = new HashMap<Integer, List<MessageContent>>();
                    messageContentMap.put(messageId, connectorMessageContentMap);
                }

                List<MessageContent> messageContents = connectorMessageContentMap.get(metaDataId);
                if (messageContents == null) {
                    messageContents = new ArrayList<MessageContent>();
                    connectorMessageContentMap.put(metaDataId, messageContents);
                }

                messageContents.add(new MessageContent(channelId, messageId, metaDataId, contentType, content, dataType, encrypted));
            }
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            close(resultSet);
            close(statement);
        }

        return messageContentMap;
    }

    /**
     * Get all content for a destination connector that is stored with the source connector
     */
    private List<MessageContent> getDestinationMessageContentFromSource(String channelId, long messageId, int metaDataId) {
        List<MessageContent> messageContents = new ArrayList<MessageContent>();
        ResultSet resultSet = null;
        PreparedStatement statement = null;

        try {
            statement = prepareStatement("getDestinationMessageContentFromSource", channelId);
            statement.setLong(1, messageId);

            resultSet = statement.executeQuery();

            while (resultSet.next()) {
                String content = resultSet.getString("content");
                ContentType contentType = ContentType.fromCode(resultSet.getInt("content_type"));
                String dataType = resultSet.getString("data_type");
                boolean encrypted = resultSet.getBoolean("is_encrypted");

                if ((decryptData || alwaysDecrypt.contains(contentType)) && encrypted && encryptor != null) {
                    content = encryptor.decrypt(content);
                    encrypted = false;
                }

                if (contentType == ContentType.ENCODED) {
                    contentType = ContentType.RAW;
                }

                messageContents.add(new MessageContent(channelId, messageId, metaDataId, contentType, content, dataType, encrypted));
            }
        } catch (SQLException e) {
            throw new DonkeyDaoException(e);
        } finally {
            close(resultSet);
            closeDatabaseObjectIfNeeded(statement);
        }

        return messageContents;
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
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        try {
            Map<String, Object> values = new HashMap<String, Object>();
            values.put("localChannelId", getLocalChannelId(channelId));

            // do not cache this statement since metadata columns may be added/removed
            statement = connection.prepareStatement(querySource.getQuery("getMetaDataMap", values));
            statement.setLong(1, messageId);
            statement.setInt(2, metaDataId);

            Map<String, Object> metaDataMap = new HashMap<String, Object>();
            resultSet = statement.executeQuery();

            if (resultSet.next()) {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                int columnCount = resultSetMetaData.getColumnCount();

                for (int i = 1; i <= columnCount; i++) {
                    MetaDataColumnType metaDataColumnType = MetaDataColumnType.fromSqlType(resultSetMetaData.getColumnType(i));
                    Object value = null;

                    switch (metaDataColumnType) {//@formatter:off
                        case STRING: value = resultSet.getString(i); break;
                        case NUMBER: value = resultSet.getBigDecimal(i); break;
                        case BOOLEAN: value = resultSet.getBoolean(i); break;
                        case TIMESTAMP:
                            
                            Timestamp timestamp = resultSet.getTimestamp(i);
                            if (timestamp != null) {
                                value = Calendar.getInstance();
                                ((Calendar) value).setTimeInMillis(timestamp.getTime());
                            }
                            break;

                        default: throw new Exception("Unrecognized MetaDataColumnType");
                    } //@formatter:on

                    metaDataMap.put(resultSetMetaData.getColumnName(i).toUpperCase(), value);
                }
            }

            return metaDataMap;
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        } finally {
            close(resultSet);
            close(statement);
        }
    }

    private Map<Long, Map<Integer, Map<String, Object>>> getMetaDataMaps(String channelId, List<Long> messageIds) {
        if (messageIds.size() > 1000) {
            throw new DonkeyDaoException("Only up to 1000 message Ids at a time are supported.");
        }

        Map<Long, Map<Integer, Map<String, Object>>> metaDataMaps = new HashMap<Long, Map<Integer, Map<String, Object>>>();
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        try {
            Map<String, Object> values = new HashMap<String, Object>();
            values.put("localChannelId", getLocalChannelId(channelId));
            values.put("messageIds", StringUtils.join(messageIds, ","));

            // do not cache this statement since metadata columns may be added/removed
            statement = connection.prepareStatement(querySource.getQuery("getMetaDataMapByMessageId", values));
            resultSet = statement.executeQuery();

            while (resultSet.next()) {
                Long messageId = resultSet.getLong("message_id");
                Integer metaDataId = resultSet.getInt("metadata_id");

                Map<Integer, Map<String, Object>> connectorMetaDataMap = metaDataMaps.get(messageId);
                if (connectorMetaDataMap == null) {
                    connectorMetaDataMap = new HashMap<Integer, Map<String, Object>>();
                    metaDataMaps.put(messageId, connectorMetaDataMap);
                }

                Map<String, Object> metaDataMap = connectorMetaDataMap.get(metaDataId);
                if (metaDataMap == null) {
                    metaDataMap = new HashMap<String, Object>();
                    connectorMetaDataMap.put(metaDataId, metaDataMap);
                }

                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                int columnCount = resultSetMetaData.getColumnCount();

                for (int i = 1; i <= columnCount; i++) {
                    MetaDataColumnType metaDataColumnType = MetaDataColumnType.fromSqlType(resultSetMetaData.getColumnType(i));
                    Object value = null;

                    switch (metaDataColumnType) {//@formatter:off
                        case STRING: value = resultSet.getString(i); break;
                        case NUMBER: value = resultSet.getBigDecimal(i); break;
                        case BOOLEAN: value = resultSet.getBoolean(i); break;
                        case TIMESTAMP:
                            
                            Timestamp timestamp = resultSet.getTimestamp(i);
                            if (timestamp != null) {
                                value = Calendar.getInstance();
                                ((Calendar) value).setTimeInMillis(timestamp.getTime());
                            }
                            break;

                        default: throw new Exception("Unrecognized MetaDataColumnType");
                    } //@formatter:on

                    metaDataMap.put(resultSetMetaData.getColumnName(i).toUpperCase(), value);
                }
            }

            return metaDataMaps;
        } catch (Exception e) {
            throw new DonkeyDaoException(e);
        } finally {
            close(resultSet);
            close(statement);
        }
    }

    /**
     * When using Derby, we manually cascade the deletion of records from dependent tables rather
     * than relying on ON DELETE CASCADE behavior. Derby uses a table-level lock when cascading
     * deletes, which hinders concurrency and can increase the risk of deadlocks.
     */
    private void cascadeMessageDelete(String queryId, String channelId) throws SQLException {
        PreparedStatement statement = null;
        try {
            statement = prepareStatement(queryId, channelId);

            if (statement != null) {
                statement.executeUpdate();
            }
        } finally {
            closeDatabaseObjectIfNeeded(statement);
        }
    }

    /**
     * When using Derby, we manually cascade the deletion of records from dependent tables rather
     * than relying on ON DELETE CASCADE behavior. Derby uses a table-level lock when cascading
     * deletes, which hinders concurrency and can increase the risk of deadlocks.
     */
    private void cascadeMessageDelete(String queryId, long messageId, String channelId) throws SQLException {
        PreparedStatement statement = null;
        try {
            statement = prepareStatement(queryId, channelId);

            if (statement != null) {
                statement.setLong(1, messageId);
                statement.executeUpdate();
            }
        } finally {
            closeDatabaseObjectIfNeeded(statement);
        }
    }

    /**
     * When using Derby, we manually cascade the deletion of records from dependent tables rather
     * than relying on ON DELETE CASCADE behavior. Derby uses a table-level lock when cascading
     * deletes, which hinders concurrency and can increase the risk of deadlocks.
     */
    private void cascadeMessageDelete(String queryId, long messageId, Map<String, Object> values) throws SQLException {
        String query = querySource.getQuery(queryId, values);

        if (query != null) {
            PreparedStatement statement = null;

            try {
                statement = connection.prepareStatement(query);
                statement.setLong(1, messageId);
                statement.executeUpdate();
            } finally {
                close(statement);
            }
        }
    }

    /**
     * Returns a prepared statement from the statementSource for the given channelId.
     */
    PreparedStatement prepareStatement(String queryId, String channelId) throws SQLException {
        Long localChannelId = null;

        if (channelId != null) {
            localChannelId = getLocalChannelId(channelId);
        }

        return statementSource.getPreparedStatement(queryId, localChannelId);
    }

    protected void close(Statement statement) {
        try {
            DbUtils.close(statement);
        } catch (SQLException e) {
            logger.error("Failed to close JDBC statement", e);
        }
    }

    protected void close(ResultSet resultSet) {
        try {
            DbUtils.close(resultSet);
        } catch (SQLException e) {
            logger.error("Failed to close JDBC result set", e);
        }
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

    private byte[] buildPrimaryKeyOfAttachment(long messageId, String attachmentId) {
        byte[] tmp = attachmentId.getBytes(UTF_8);
        byte[] data = new byte[8 + tmp.length];
        longToBytes(messageId, data, 0);
        System.arraycopy(tmp, 0, data, 8, tmp.length);
        
        return data;
    }
}
