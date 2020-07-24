package com.mirth.connect.server.controllers.je;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ibatis.session.SqlSession;
import org.apache.log4j.Logger;

import com.mirth.connect.donkey.model.message.ContentType;
import com.mirth.connect.donkey.server.BdbJeDataSource;
import com.mirth.connect.donkey.server.controllers.ChannelController;
import com.mirth.connect.model.filters.MessageFilter;
import com.mirth.connect.model.filters.elements.ContentSearchElement;
import com.mirth.connect.model.filters.elements.MetaDataSearchElement;
import com.mirth.connect.server.ExtensionLoader;
import com.mirth.connect.server.controllers.DonkeyMessageController;
import com.mirth.connect.server.controllers.MessageController;
import com.mirth.connect.server.controllers.je.msgsearch.CustomeMetadataSelector;
import com.mirth.connect.server.controllers.je.msgsearch.MessageContentSelector;
import com.mirth.connect.server.controllers.je.msgsearch.MessageSelector;
import com.mirth.connect.server.controllers.je.msgsearch.MetadataSelector;
import com.mirth.connect.server.controllers.DonkeyMessageController.FilterOptions;
import com.mirth.connect.server.mybatis.MessageSearchResult;
import com.mirth.connect.server.mybatis.MessageTextResult;
import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;

public class BdbJeMessageController extends DonkeyMessageController {
    private Environment env;
    public BdbJeDataSource ds;

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
        try {
            long count = 0;
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

            return count;
        } finally {
            if(txn != null) {
                txn.commit();
            }
            long endTime = System.currentTimeMillis();
            logger.debug("Count executed in " + (endTime - startTime) + "ms");
        }
    }

    private Map<Long, MessageSearchResult> searchAll(Transaction txn, MessageFilter filter, Long localChannelId, boolean includeMessageData, FilterOptions filterOptions, long minMessageId, long maxMessageId) {
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

                Map<String, Object> contentParams = new HashMap<String, Object>();
                contentParams.put("localChannelId", localChannelId);
                contentParams.put("includedMetaDataIds", filter.getIncludedMetaDataIds());
                contentParams.put("excludedMetaDataIds", filter.getExcludedMetaDataIds());
                contentParams.put("minMessageId", potentialMin);
                contentParams.put("maxMessageId", potentialMax);

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
                    searchText(session, new HashMap<String, Object>(contentParams), potentialMessages, textMessages, filter.getTextSearchRegex(), filter.getTextSearch(), filter.getTextSearchMetaDataColumns());

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

    private static List<Field> getNonNullFields(MessageFilter filter) throws IllegalAccessException {
        List<Field> lst = new ArrayList<>();
        for(Field f : MSG_FILTER_FIELDS) {
            if(f.get(filter) != null) {
                lst.add(f);
            }
        }
        return lst;
    }
}
