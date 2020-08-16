package com.mirth.connect.server.controllers.je;

import static com.mirth.connect.donkey.util.SerializerUtil.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.SqlSession;
import org.apache.log4j.Logger;

import com.mirth.commons.encryption.Encryptor;
import com.mirth.connect.client.core.ControllerException;
import com.mirth.connect.donkey.model.message.ConnectorMessage;
import com.mirth.connect.donkey.model.message.ContentType;
import com.mirth.connect.donkey.model.message.Message;
import com.mirth.connect.donkey.model.message.MessageContent;
import com.mirth.connect.donkey.model.message.RawMessage;
import com.mirth.connect.donkey.model.message.CapnpModel.CapAttachment;
import com.mirth.connect.donkey.model.message.attachment.Attachment;
import com.mirth.connect.donkey.model.message.attachment.AttachmentHandlerProvider;
import com.mirth.connect.donkey.server.BdbJeDataSource;
import com.mirth.connect.donkey.server.Constants;
import com.mirth.connect.donkey.server.channel.Channel;
import com.mirth.connect.donkey.server.channel.ChannelException;
import com.mirth.connect.donkey.server.controllers.ChannelController;
import com.mirth.connect.donkey.server.data.DonkeyDao;
import com.mirth.connect.donkey.server.message.DataType;
import com.mirth.connect.donkey.util.MapUtil;
import com.mirth.connect.donkey.util.xstream.SerializerException;
import com.mirth.connect.model.converters.ObjectXMLSerializer;
import com.mirth.connect.model.filters.MessageFilter;
import com.mirth.connect.model.filters.elements.ContentSearchElement;
import com.mirth.connect.model.filters.elements.MetaDataSearchElement;
import com.mirth.connect.server.ExtensionLoader;
import com.mirth.connect.server.channel.ErrorTaskHandler;
import com.mirth.connect.server.controllers.ConfigurationController;
import com.mirth.connect.server.controllers.ControllerFactory;
import com.mirth.connect.server.controllers.DonkeyMessageController;
import com.mirth.connect.server.controllers.EngineController;
import com.mirth.connect.server.controllers.ExtensionController;
import com.mirth.connect.server.controllers.MessageController;
import com.mirth.connect.server.controllers.je.msgsearch.CustomeMetadataSelector;
import com.mirth.connect.server.controllers.je.msgsearch.MessageContentSelector;
import com.mirth.connect.server.controllers.je.msgsearch.MessageSelector;
import com.mirth.connect.server.controllers.je.msgsearch.MetadataSelector;
import com.mirth.connect.server.controllers.DonkeyMessageController.FilterOptions;
import com.mirth.connect.server.mybatis.MessageSearchResult;
import com.mirth.connect.server.mybatis.MessageTextResult;
import com.mirth.connect.server.util.DICOMMessageUtil;
import com.mirth.connect.server.util.ListRangeIterator;
import com.mirth.connect.server.util.SqlConfig;
import com.mirth.connect.server.util.ListRangeIterator.ListRangeItem;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class BdbJeMessageController extends DonkeyMessageController {
    private Environment env;
    public BdbJeDataSource ds;
    private String _serverId;

    private Logger logger = Logger.getLogger(BdbJeMessageController.class);
    
    private static final Field[] MSG_FILTER_FIELDS;
    static {
        MSG_FILTER_FIELDS = MessageFilter.class.getDeclaredFields();
        for(Field f : MSG_FILTER_FIELDS) {
            f.setAccessible(true);
        }
    }

    protected BdbJeMessageController() {
    }

    public static MessageController create() {
        synchronized (BdbJeMessageController.class) {
            if (instance == null) {
                instance = ExtensionLoader.getInstance().getControllerInstance(MessageController.class);

                if (instance == null) {
                    BdbJeMessageController i = new BdbJeMessageController();
                    BdbJeDataSource ds = BdbJeDataSource.getInstance();
                    i.ds = ds;
                    i.env = ds.getBdbJeEnv();

                    instance = i;
                }
            }

            return instance;
        }
    }

    @Override
    public Long getMessageCount(MessageFilter filter, String channelId) {
        if (filter.getIncludedMetaDataIds() != null && filter.getIncludedMetaDataIds().isEmpty() && filter.getExcludedMetaDataIds() == null) {
            return 0L;
        }

        long startTime = System.currentTimeMillis();

        FilterOptions filterOptions = new FilterOptions(filter, channelId, true);
        long maxMessageId = filterOptions.getMaxMessageId();
        long minMessageId = filterOptions.getMinMessageId();

        Long localChannelId = ChannelController.getInstance().getLocalChannelId(channelId, true);

        Transaction txn = env.beginTransaction(null, null);
        long count = 0;
        try {
            long batchSize = 50000;

            while (maxMessageId >= minMessageId) {
                /*
                 * Search in descending order so that messages will be counted from the greatest to
                 * lowest message id
                 */
                long currentMinMessageId = Math.max(maxMessageId - batchSize + 1, minMessageId);
                
                maxMessageId -= batchSize;

                Map<Long, MessageSearchResult> foundMessages = searchAll(txn, filter, localChannelId, false, filterOptions, currentMinMessageId, maxMessageId);

                count += foundMessages.size();
            }
        }
        catch(Exception e) {
            logger.warn("failed to search database", e);
        }
        finally {
            if(txn != null) {
                txn.commit();
            }
            long endTime = System.currentTimeMillis();
            logger.debug("Count executed in " + (endTime - startTime) + "ms");
        }

        return count;
    }

    @Override
    public Message getMessageContent(String channelId, Long messageId, List<Integer> metaDataIds) {
        DonkeyDao dao = null;

        try {
            long localChannelId = ChannelController.getInstance().getLocalChannelId(channelId, true);

            Message message = null;
            Transaction txn = env.beginTransaction(null, null);
            try {
                message = MessageSelector.selectMessageById(txn, getServerId(), localChannelId, messageId);
                txn.commit();
            }
            catch(Exception e) {
                txn.abort();
            }

            if (message != null) {
                message.setChannelId(channelId);
            }

            dao = getDao(true);
            Map<Integer, ConnectorMessage> connectorMessages = dao.getConnectorMessages(channelId, messageId, metaDataIds);

            for (Entry<Integer, ConnectorMessage> connectorMessageEntry : connectorMessages.entrySet()) {
                Integer metaDataId = connectorMessageEntry.getKey();
                ConnectorMessage connectorMessage = connectorMessageEntry.getValue();

                message.getConnectorMessages().put(metaDataId, connectorMessage);
            }

            return message;
        } finally {
            dao.close();
        }
    }

    @Override
    public List<Attachment> getMessageAttachmentIds(String channelId, Long messageId, boolean readOnly) {
        long localChannelId = ChannelController.getInstance().getLocalChannelId(channelId, true);
        List<Attachment> lst = new ArrayList<>();

        Cursor cursor = null;
        Transaction txn = null;
        BdbJeDataSource ds = BdbJeDataSource.getInstance();
        Database atDb = ds.getDbMap().get("d_ma" + localChannelId);
        try {
            txn = ds.getBdbJeEnv().beginTransaction(null, null);
            cursor = atDb.openCursor(txn, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            key.setData(longToBytes(messageId));
            OperationStatus os = cursor.getSearchKeyRange(key, data, null);
            if(os == OperationStatus.SUCCESS) {
                // step back
                cursor.getPrev(key, data, null);
            }
            
            while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS) {
                long id = bytesToLong(key.getData());
                if(id != messageId) {
                    break;
                }
                CapAttachment.Reader cr = (CapAttachment.Reader)readMessage(data.getData()).getRoot(CapAttachment.factory);
                Attachment at = new Attachment();
                at.setId(cr.getId().toString());
                at.setType(cr.getType().toString());
                lst.add(at);
            }
            txn.commit();
        }
        catch(Exception e) {
            if(txn != null) {
                txn.abort();
            }
            logger.warn("", e);
        }
        finally {
            if(cursor != null) {
                cursor.close();
            }
        }
        return lst;
    }

    @Override
    public void removeMessages(String channelId, MessageFilter filter) {
        EngineController engineController = ControllerFactory.getFactory().createEngineController();

        FilterOptions filterOptions = new FilterOptions(filter, channelId, false);
        long maxMessageId = filterOptions.getMaxMessageId();
        long minMessageId = filterOptions.getMinMessageId();

        Long localChannelId = ChannelController.getInstance().getLocalChannelId(channelId);
        /*
         * Include the processed boolean with the result set in order to determine whether the
         * message can be deleted if the channel is not stopped
         */

        long batchSize = 50000;

        while (maxMessageId >= minMessageId) {
            /*
             * Search in descending order so that messages will be deleted from the greatest to
             * lowest message id
             */
            long currentMinMessageId = Math.max(maxMessageId - batchSize + 1, minMessageId);
            maxMessageId -= batchSize;

            Map<Long, MessageSearchResult> results = null;
            Transaction txn = env.beginTransaction(null, null);
            try {
                //Map<Long, MessageSearchResult> results = searchAll(session, params, filter, localChannelId, true, filterOptions);
                results = searchAll(txn, filter, localChannelId, true, filterOptions, currentMinMessageId, maxMessageId);
                txn.commit();
            }
            catch(Exception e) {
                txn.abort();
                logger.warn("", e);
            }

            if(results != null) {
                ErrorTaskHandler handler = new ErrorTaskHandler();
                engineController.removeMessages(channelId, results, handler);
                if (handler.isErrored()) {
                    logger.error("Remove messages task terminated due to error or halt.", handler.getError());
                    break;
                }
            }
        }

        Channel channel = engineController.getDeployedChannel(channelId);
        if (channel != null) {
            // Invalidate the queue buffer to ensure stats are updated.
            channel.invalidateQueues();
        }
    }

    public void reprocessMessages(String channelId, MessageFilter filter, boolean replace, Collection<Integer> reprocessMetaDataIds) throws ControllerException {
        EngineController engineController = ControllerFactory.getFactory().createEngineController();
        Channel deployedChannel = engineController.getDeployedChannel(channelId);
        if (deployedChannel == null) {
            throw new ControllerException("Channel is no longer deployed!");
        }

        AttachmentHandlerProvider attachmentHandlerProvider = deployedChannel.getAttachmentHandlerProvider();
        DataType dataType = deployedChannel.getSourceConnector().getInboundDataType();
        boolean isBinary = ExtensionController.getInstance().getDataTypePlugins().get(dataType.getType()).isBinary();
        Encryptor encryptor = ConfigurationController.getInstance().getEncryptor();

        FilterOptions filterOptions = new FilterOptions(filter, channelId, false);
        long maxMessageId = filterOptions.getMaxMessageId();
        long minMessageId = filterOptions.getMinMessageId();

        Long localChannelId = ChannelController.getInstance().getLocalChannelId(channelId);
        /*
         * Include the import id with the result set in order to determine whether the message was
         * imported
         */
        long batchSize = 50000;

        while (maxMessageId >= minMessageId) {
            /*
             * Search in ascending order so that messages will be reprocessed from the lowest to
             * greatest message id
             */
            long currentMaxMessageId = Math.min(minMessageId + batchSize - 1, maxMessageId);
            minMessageId += batchSize;

            Map<Long, MessageSearchResult> results = null;
            Transaction txn = env.beginTransaction(null, null);
            try {
                //Map<Long, MessageSearchResult> results = searchAll(session, params, filter, localChannelId, true, filterOptions);
                results = searchAll(txn, filter, localChannelId, true, filterOptions, minMessageId, currentMaxMessageId);
                txn.commit();
            }
            catch(Exception e) {
                txn.abort();
                logger.warn("", e);
            }

            Map<Long, MessageSearchResult> foundMessages = new TreeMap<Long, MessageSearchResult>();
            if(results != null) {
                foundMessages.putAll(results);
            }

            for (Entry<Long, MessageSearchResult> entry : foundMessages.entrySet()) {
                Long messageId = entry.getKey();
                Long importId = entry.getValue().getImportId();

                List<MessageContent> contentList = null;
                //List<MessageContent> contentList = session.selectList("Message.selectMessageForReprocessing", params);
                // TODO optimization - find a way to handle all this with a single transaction handle 
                txn = env.beginTransaction(null, null);
                try {
                    contentList = MessageContentSelector.selectMessageForReprocessing(txn, localChannelId, messageId);
                    txn.commit();
                }
                catch(Exception e) {
                    txn.abort();
                    logger.warn("", e);
                }

                MessageContent rawContent = null;
                MessageContent sourceMapContent = null;

                if (contentList != null) {
                    for (MessageContent content : contentList) {
                        if (content.getContentType() == ContentType.RAW) {
                            rawContent = content;
                        } else if (content.getContentType() == ContentType.SOURCE_MAP) {
                            sourceMapContent = content;
                        }
                    }
                }

                if (rawContent != null) {
                    if (rawContent.isEncrypted()) {
                        rawContent.setContent(encryptor.decrypt(rawContent.getContent()));
                        rawContent.setEncrypted(false);
                    }

                    ConnectorMessage connectorMessage = new ConnectorMessage();
                    connectorMessage.setChannelId(channelId);
                    connectorMessage.setMessageId(messageId);
                    connectorMessage.setMetaDataId(0);
                    connectorMessage.setRaw(rawContent);

                    RawMessage rawMessage = null;

                    if (isBinary) {
                        rawMessage = new RawMessage(DICOMMessageUtil.getDICOMRawBytes(connectorMessage));
                    } else {
                        rawMessage = new RawMessage(org.apache.commons.codec.binary.StringUtils.newString(attachmentHandlerProvider.reAttachMessage(rawContent.getContent(), connectorMessage, Constants.ATTACHMENT_CHARSET, false, true, true), Constants.ATTACHMENT_CHARSET));
                    }

                    rawMessage.setOverwrite(replace);
                    rawMessage.setImported(importId != null);
                    rawMessage.setOriginalMessageId(messageId);

                    try {
                        Map<String, Object> sourceMap = rawMessage.getSourceMap();
                        if (sourceMapContent != null && sourceMapContent.getContent() != null) {
                            if (sourceMapContent.isEncrypted()) {
                                sourceMapContent.setContent(encryptor.decrypt(sourceMapContent.getContent()));
                                sourceMapContent.setEncrypted(false);
                            }

                            /*
                             * We do putAll instead of setting the source map directly here because
                             * the previously stored map will be unmodifiable. We need to set the
                             * destination metadata IDs after this, so the map needs to be
                             * modifiable.
                             */
                            sourceMap.putAll(MapUtil.deserializeMap(ObjectXMLSerializer.getInstance(), sourceMapContent.getContent()));
                        }

                        sourceMap.put(Constants.REPROCESSED_KEY, true);
                        sourceMap.put(Constants.REPLACED_KEY, replace);

                        // Set the destination metadata ID list here to overwrite anything that was previously stored 
                        rawMessage.setDestinationMetaDataIds(reprocessMetaDataIds);

                        engineController.dispatchRawMessage(channelId, rawMessage, true, false);
                    } catch (SerializerException e) {
                        logger.error("Could not reprocess message " + messageId + " for channel " + channelId + " because the source map content is invalid.", e);
                    } catch (ChannelException e) {
                        if (e.isStopped()) {
                            // This should only return true if the entire channel is stopped, since we are forcing the message even if the source connector is stopped.
                            logger.error("Reprocessing job cancelled because the channel is stopping or stopped.", e);
                            return;
                        }
                    } catch (Throwable e) {
                        // Do nothing. An error should have been logged.
                    }
                } else {
                    logger.error("Could not reprocess message " + messageId + " for channel " + channelId + " because no source raw content was found. The content may have been pruned or the channel may not be configured to store raw content.");
                }
            }
        }
    }

    protected List<MessageSearchResult> searchMessages(MessageFilter filter, String channelId, int offset, int limit) {
        long startTime = System.currentTimeMillis();

        FilterOptions filterOptions = new FilterOptions(filter, channelId, true);
        long maxMessageId = filterOptions.getMaxMessageId();
        long minMessageId = filterOptions.getMinMessageId();

        Long localChannelId = ChannelController.getInstance().getLocalChannelId(channelId, true);

        List<MessageSearchResult> results = null;
        Transaction txn = env.beginTransaction(null, null);
        try {
            NavigableMap<Long, MessageSearchResult> messages = new TreeMap<Long, MessageSearchResult>();

            int offsetRemaining = offset;
            /*
             * If the limit is greater than the default batch size, use the limit, but cap it at
             * 50000.
             */
            long batchSize = Math.min(Math.max(limit, 500), 50000);
            long totalSearched = 0;

            while (messages.size() < limit && maxMessageId >= minMessageId) {
                /*
                 * Slowly increase the batch size in case all the necessary results are found early
                 * on.
                 */
                if (totalSearched >= 100000 && batchSize < 50000) {
                    batchSize = 50000;
                } else if (totalSearched >= 10000 && batchSize < 10000) {
                    batchSize = 10000;
                } else if (totalSearched >= 1000 && batchSize < 1000) {
                    batchSize = 1000;
                }

                /*
                 * Search in descending order so that messages will be found from the greatest to
                 * lowest message id
                 */
                long currentMinMessageId = Math.max(maxMessageId - batchSize + 1, minMessageId);
                long curMaxMsgId = maxMessageId; // to preserve the value before it becomes negative
                maxMessageId -= batchSize;
                totalSearched += batchSize;

                Map<Long, MessageSearchResult> foundMessages = searchAll(txn, filter, localChannelId, false, filterOptions, currentMinMessageId, curMaxMsgId);

                if (!foundMessages.isEmpty()) {
                    /*
                     * Skip results until there is no offset remaining. This is required when
                     * viewing results beyond the first page
                     */
                    if (offsetRemaining >= foundMessages.size()) {
                        offsetRemaining -= foundMessages.size();
                    } else if (offsetRemaining == 0) {
                        messages.putAll(foundMessages);
                    } else {
                        NavigableMap<Long, MessageSearchResult> orderedMessages = new TreeMap<Long, MessageSearchResult>(foundMessages);

                        while (offsetRemaining-- > 0) {
                            orderedMessages.pollLastEntry();
                        }

                        messages.putAll(orderedMessages);
                    }
                }
            }

            // Remove results beyond the limit requested
            while (messages.size() > limit) {
                messages.pollFirstEntry();
            }

            results = new ArrayList<MessageSearchResult>(messages.size());

            /*
             * Now that we have the message and metadata ids that should be returned as the result,
             * we need to retrieve the message data for those.
             */
            if (!messages.isEmpty()) {
                Iterator<Long> iterator = messages.descendingKeySet().iterator();

                while (iterator.hasNext()) {
                    ListRangeIterator listRangeIterator = new ListRangeIterator(iterator, ListRangeIterator.DEFAULT_LIST_LIMIT, false, null);

                    while (listRangeIterator.hasNext()) {
                        ListRangeItem item = listRangeIterator.next();
                        List<Long> list = item.getList();
                        Long startRange = item.getStartRange();
                        Long endRange = item.getEndRange();

                        if (list != null || (startRange != null && endRange != null)) {
                            Long curMinMessageId = null;
                            Long curMaxMessageId = null;
                            if (list == null) {
                                curMinMessageId = endRange;
                                curMaxMessageId = startRange;
                            }

                            // Get the current batch of results
                            //List<MessageSearchResult> currentResults = session.selectList("Message.selectMessagesById", messageParams);
                            List<MessageSearchResult> currentResults = MessageSelector.selectMessagesById(txn, getServerId(), localChannelId, list, curMinMessageId, curMaxMessageId);

                            // Add the metadata ids to each result
                            for (MessageSearchResult currentResult : currentResults) {
                                currentResult.setMetaDataIdSet(messages.get(currentResult.getMessageId()).getMetaDataIdSet());
                            }

                            // Add the current batch to the final list of results
                            results.addAll(currentResults);
                        }
                    }
                }
            }
        }
        catch(Exception e) {
            logger.warn("failed to search messages", e);
        }
        finally {
            if(txn != null) {
                txn.commit();
            }
            long endTime = System.currentTimeMillis();
            logger.debug("Search executed in " + (endTime - startTime) + "ms");
        }

        if(results == null) {
            results = new ArrayList<>();
        }

        return results;
    }

    private Map<Long, MessageSearchResult> searchAll(Transaction txn, MessageFilter filter, Long localChannelId, boolean includeMessageData, FilterOptions filterOptions, long minMessageId, long maxMessageId) throws Exception {
        Map<Long, MessageSearchResult> foundMessages = new HashMap<Long, MessageSearchResult>();

        List<Field> nonNullFilterfields = getNonNullFields(filter);
        // Search the message table to find which message ids meet the search criteria.
        List<MessageTextResult> messageResults = MessageSelector.searchMessageTable(txn, filter, nonNullFilterfields, localChannelId, filterOptions, minMessageId, maxMessageId);
        /*
         * If the message table search provided no records then there is no need to perform any more
         * searches on this range of message ids.
         */
        if (!messageResults.isEmpty()) {
            Set<Long> messageIdSet = new HashSet<Long>(messageResults.size());
            for (MessageTextResult messageResult : messageResults) {
                messageIdSet.add(messageResult.getMessageId());
            }

            /*
             * Search the metadata table to find which message and metadataids meet the search
             * criteria. If a text search is being performed, we also check the connector name
             * column while we're at it.
             */

            //session.selectList("Message.searchMetaDataTable", params);
            List<MessageTextResult> metaDataResults = MetadataSelector.searchMetaDataTable(txn, filter, nonNullFilterfields, localChannelId, filterOptions, minMessageId, maxMessageId);

            /*
             * Messages that matched the text search criteria. Since text search spans across
             * multiple tables (metadata, content, custom metadata), the map is created it can be
             * used for the searches on each table.
             */
            Map<Long, MessageSearchResult> textMessages = new HashMap<Long, MessageSearchResult>();
            /*
             * Messages that met the criteria on the message and metadata tables and still need to
             * have lengthy search run on them.
             */
            Map<Long, MessageSearchResult> potentialMessages = new HashMap<Long, MessageSearchResult>();
            for (MessageTextResult metaDataResult : metaDataResults) {
                if (messageIdSet.contains(metaDataResult.getMessageId())) {
                    if (filterOptions.isSearchText() && metaDataResult.isTextFound() != null && metaDataResult.isTextFound()) {
                        /*
                         * Text search was found in the metadata table so add the message/metadata
                         * id to the text messages.
                         */
                        addMessageToMap(textMessages, metaDataResult.getMessageId(), metaDataResult.getMetaDataId());

                        if (filterOptions.isSearchCustomMetaData() || filterOptions.isSearchContent()) {
                            /*
                             * If content or custom metadata is being searched, still add the
                             * message to potentialMessages so the lengthy search will be run.
                             */
                            addMessageToMap(potentialMessages, metaDataResult.getMessageId(), metaDataResult.getMetaDataId());
                        }
                    } else if (filterOptions.isSearchCustomMetaData() || filterOptions.isSearchContent() || filterOptions.isSearchText()) {
                        /*
                         * If no text search was found and any lengthy search is required, add the
                         * message to potentialMessages.
                         */
                        addMessageToMap(potentialMessages, metaDataResult.getMessageId(), metaDataResult.getMetaDataId());
                    } else {
                        /*
                         * If no lengthy search is required, just add the message to foundMesages.
                         */
                        addMessageToMap(foundMessages, metaDataResult.getMessageId(), metaDataResult.getMetaDataId());
                    }
                }
            }

            // These are no longer used so allow GC to reclaim their memory
            metaDataResults = null;
            messageIdSet = null;
            if (!includeMessageData) {
                messageResults = null;
            }

            if (potentialMessages.isEmpty()) {
                // If lengthy search is not being run, add all text messages to found messages
                foundMessages.putAll(textMessages);
            } else {
                long potentialMin = Long.MAX_VALUE;
                long potentialMax = Long.MIN_VALUE;

                for (long key : potentialMessages.keySet()) {
                    if (key < potentialMin) {
                        potentialMin = key;
                    }
                    if (key > potentialMax) {
                        potentialMax = key;
                    }
                }

                boolean searchCustomMetaData = filterOptions.isSearchCustomMetaData();
                boolean searchContent = filterOptions.isSearchContent();
                boolean searchText = filterOptions.isSearchText();

                /*
                 * The map of messages that contains the combined results from all of the lengthy
                 * searches. For all searches that are being performed, a message and metadata id
                 * must be found in all three in order for the message to remain in this map
                 */
                Map<Long, MessageSearchResult> tempMessages = null;

                if (searchCustomMetaData) {
                    tempMessages = new HashMap<Long, MessageSearchResult>();
                    // Perform the custom metadata search
                    //searchCustomMetaData(session, new HashMap<String, Object>(contentParams), potentialMessages, tempMessages, filter.getMetaDataSearch());
                    searchCustomMetaData(txn, filter, nonNullFilterfields, potentialMessages, tempMessages, filter.getMetaDataSearch(), localChannelId, potentialMin, potentialMax);

                    /*
                     * If tempMessages is empty, there is no need to search on either the content or
                     * text because the join will never return any results
                     */
                    if (tempMessages.isEmpty()) {
                        searchContent = false;
                        searchText = false;
                    }
                }
                if (searchContent) {
                    Map<Long, MessageSearchResult> contentMessages = new HashMap<Long, MessageSearchResult>();
                    // Perform the content search
                    searchContent(txn, potentialMessages, contentMessages, filter.getContentSearch(), localChannelId, potentialMin, potentialMax);

                    if (tempMessages == null) {
                        /*
                         * If temp messages has not been created yet, then there is no need to join
                         * the results from this search and previous searches. Just set the current
                         * messages as the temp messages
                         */
                        tempMessages = contentMessages;
                    } else {
                        /*
                         * Otherwise join the two maps so that the only results left in tempMessages
                         * are those that also exist in the current message map
                         */
                        joinMessages(tempMessages, contentMessages);
                    }

                    /*
                     * If tempMessages is empty, there is no need to search on either the text
                     * because the join will never return any results
                     */
                    if (tempMessages.isEmpty()) {
                        searchText = false;
                    }
                }
                if (searchText) {
                    // Perform the text search
                    searchText(txn, potentialMessages, textMessages, filter, nonNullFilterfields, localChannelId, potentialMin, potentialMax);

                    if (tempMessages == null) {
                        /*
                         * If temp messages has not been created yet, then there is no need to join
                         * the results from this search and previous searches. Just set the current
                         * messages as the temp messages
                         */
                        tempMessages = textMessages;
                    } else {
                        /*
                         * Otherwise join the two maps so that the only results left in tempMessages
                         * are those that also exist in the current message map
                         */
                        joinMessages(tempMessages, textMessages);
                    }
                }

                /*
                 * Add all the results from tempMessages after all the joins have been completed
                 * into foundMessages
                 */
                foundMessages.putAll(tempMessages);
            }

            /*
             * If message data was requested then copy it from the message search into the final
             * results
             */
            if (!foundMessages.isEmpty() && includeMessageData) {
                // Build a map of the message data results for quicker access
                Map<Long, MessageTextResult> messageDataResults = new HashMap<Long, MessageTextResult>(messageResults.size());
                for (MessageTextResult messageResult : messageResults) {
                    messageDataResults.put(messageResult.getMessageId(), messageResult);
                }

                /*
                 * For each found result, copy over any message data that may have been retrieved
                 * already
                 */
                for (Entry<Long, MessageSearchResult> entry : foundMessages.entrySet()) {
                    Long messageId = entry.getKey();
                    MessageSearchResult result = entry.getValue();

                    MessageTextResult textResult = messageDataResults.get(messageId);
                    if (textResult != null) {
                        result.setImportId(textResult.getImportId());
                        result.setProcessed(textResult.getProcessed());
                    }
                }
            }
        }

        return foundMessages;
    }

    private void searchCustomMetaData(Transaction txn, MessageFilter filter, List<Field> nonNullFilterfields, Map<Long, MessageSearchResult> potentialMessages, Map<Long, MessageSearchResult> customMetaDataMessages, List<MetaDataSearchElement> metaDataSearchElements, long localChannelId, long minMessageId, long maxMessageId) throws Exception {
        /*
         * Search the custom meta data table for message and metadata ids matching the metadata
         * search criteria
         */
        //session.selectList("Message.searchCustomMetaDataTable", params);
        List<MessageTextResult> results = CustomeMetadataSelector.searchCustomMetaDataTable(txn, filter, nonNullFilterfields, localChannelId, minMessageId, maxMessageId);

        for (MessageTextResult result : results) {
            Long messageId = result.getMessageId();
            Integer metaDataId = result.getMetaDataId();

            if (potentialMessages.containsKey(messageId)) {
                Set<Integer> allowedMetaDataIds = potentialMessages.get(messageId).getMetaDataIdSet();
                /*
                 * Ignore the message and metadata id if they are not allowed because they were
                 * already filtered in a previous step
                 */
                if (allowedMetaDataIds.contains(metaDataId)) {
                    addMessageToMap(customMetaDataMessages, messageId, metaDataId);
                }
            }
        }
    }

    private void searchContent(Transaction txn, Map<Long, MessageSearchResult> potentialMessages, Map<Long, MessageSearchResult> contentMessages, List<ContentSearchElement> contentSearchElements, long localChannelId, long minMessageId, long maxMessageId) throws Exception {
        int index = 0;

        while (index < contentSearchElements.size() && (index == 0 || !contentMessages.isEmpty())) {
            ContentSearchElement element = contentSearchElements.get(index);

            if (CollectionUtils.isNotEmpty(element.getSearches())) {

                /*
                 * Search the content table for message and metadata ids matching the content search
                 * criteria
                 */
                //List<MessageTextResult> results = session.selectList("Message.searchContentTable", params);
                List<MessageTextResult> results = MessageContentSelector.searchContentTable(txn, element, null, element.getContentCode(), localChannelId, minMessageId, maxMessageId);

                Map<Long, MessageSearchResult> tempMessages = new HashMap<Long, MessageSearchResult>();

                for (MessageTextResult result : results) {
                    Long messageId = result.getMessageId();
                    Integer metaDataId = result.getMetaDataId();

                    if (potentialMessages.containsKey(messageId)) {
                        Set<Integer> allowedMetaDataIds = potentialMessages.get(messageId).getMetaDataIdSet();
                        /*
                         * Ignore the message and metadata id if they are not allowed because they
                         * were already filtered in a previous step
                         */
                        if (allowedMetaDataIds.contains(metaDataId)) {
                            if (index == 0) {
                                /*
                                 * For the first search, add the results to the final result map
                                 * since there is nothing to join from
                                 */
                                addMessageToMap(contentMessages, messageId, metaDataId);
                            } else {
                                /*
                                 * For other searches, add the results to the temp result map so
                                 * they can be joined with the final result map
                                 */
                                addMessageToMap(tempMessages, messageId, metaDataId);
                            }
                        }
                    }
                }

                /*
                 * If the raw content is being searched, perform an additional search on the source
                 * encoded content since the destination
                 */
                if (ContentType.fromCode(element.getContentCode()) == ContentType.RAW) {

                    //results = session.selectList("Message.searchContentTable", params);
                    results = MessageContentSelector.searchContentTable(txn, element, 0, ContentType.ENCODED.getContentTypeCode(), localChannelId, minMessageId, maxMessageId);

                    for (MessageTextResult result : results) {
                        Long messageId = result.getMessageId();

                        if (potentialMessages.containsKey(messageId)) {
                            Set<Integer> allowedMetaDataIds = potentialMessages.get(messageId).getMetaDataIdSet();
                            for (Integer allowedMetaDataId : allowedMetaDataIds) {
                                if (allowedMetaDataId != 0) {
                                    /*
                                     * If the source encoded is found, then all destinations have
                                     * matched on the raw content, so all allowed metadata ids other
                                     * than 0 (source) need to be added
                                     */
                                    if (index == 0) {
                                        /*
                                         * For the first search, add the results to the final result
                                         * map since there is nothing to join from
                                         */
                                        addMessageToMap(contentMessages, messageId, allowedMetaDataId);
                                    } else {
                                        /*
                                         * For other searches, add the results to the temp result
                                         * map so they can be joined with the final result map
                                         */
                                        addMessageToMap(tempMessages, messageId, allowedMetaDataId);
                                    }
                                }
                            }
                        }
                    }
                }

                if (index > 0) {
                    /*
                     * If there are more than one searches performed, join the results since the
                     * message and metadata ids must be found in all searches in order to be
                     * considered "found"
                     */
                    joinMessages(contentMessages, tempMessages);
                }
            }

            index++;
        }
    }

    private void searchText(Transaction txn, Map<Long, MessageSearchResult> potentialMessages, Map<Long, MessageSearchResult> textMessages, MessageFilter filter, List<Field> nonNullFilterfields, long localChannelId, long minMessageId, long maxMessageId) throws Exception {
        List<MessageTextResult> results;

        if (CollectionUtils.isNotEmpty(filter.getTextSearchMetaDataColumns())) {
            /*
             * Search the custom meta data table for message and metadata ids matching the text
             * search criteria
             */
            //results = session.selectList("Message.searchCustomMetaDataTable", params);
            results = CustomeMetadataSelector.searchCustomMetaDataTable(txn, filter, nonNullFilterfields, localChannelId, minMessageId, maxMessageId);

            for (MessageTextResult result : results) {
                Long messageId = result.getMessageId();
                Integer metaDataId = result.getMetaDataId();

                if (potentialMessages.containsKey(messageId)) {
                    Set<Integer> allowedMetaDataIds = potentialMessages.get(messageId).getMetaDataIdSet();
                    /*
                     * Ignore the message and metadata id if they are not allowed because they were
                     * already filtered in a previous step
                     */
                    if (allowedMetaDataIds.contains(metaDataId)) {
                        addMessageToMap(textMessages, messageId, metaDataId);
                    }
                }
            }
        }

        /*
         * Search the content table for message and metadata ids matching the text search criteria
         */
        //results = session.selectList("Message.searchContentTable", params);
        ContentSearchElement element = new ContentSearchElement(-1, Collections.singletonList(filter.getTextSearch()));
        results = MessageContentSelector.searchContentTable(txn, element, null, null, localChannelId, minMessageId, maxMessageId);

        for (MessageTextResult result : results) {
            Long messageId = result.getMessageId();
            Integer metaDataId = result.getMetaDataId();
            Integer contentCode = result.getContentType();
            ContentType contentType = ContentType.fromCode(contentCode);

            if (potentialMessages.containsKey(messageId)) {
                Set<Integer> allowedMetaDataIds = potentialMessages.get(messageId).getMetaDataIdSet();
                if (metaDataId == 0 && contentType == ContentType.ENCODED) {
                    /*
                     * If the text search is found in the source encoded content, then all the
                     * allowed destinations would match on the raw content so all allowed metadata
                     * ids for this message need to be added
                     */
                    for (Integer allowedMetaDataId : allowedMetaDataIds) {
                        addMessageToMap(textMessages, messageId, allowedMetaDataId);
                    }
                } else if (allowedMetaDataIds.contains(metaDataId)) {
                    /*
                     * Ignore the message and metadata id if they are not allowed because they were
                     * already filtered in a previous step
                     */
                    addMessageToMap(textMessages, messageId, metaDataId);
                }
            }
        }
    }

    private static List<Field> getNonNullFields(MessageFilter filter) throws IllegalAccessException {
        List<Field> lst = new ArrayList<>();
        for(Field f : MSG_FILTER_FIELDS) {
            if(f.get(filter) != null) {
                lst.add(f);
            }
        }
        return lst;
    }
    
    private String getServerId() {
        if(_serverId == null) {
            _serverId = ConfigurationController.getInstance().getServerId();            
        }
        
        return _serverId;
    }
}
