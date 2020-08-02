package com.mirth.connect.server.controllers.je.msgsearch;

import static com.mirth.connect.donkey.util.SerializerUtil.longToBytes;
import static com.mirth.connect.donkey.util.SerializerUtil.readMessage;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import com.mirth.connect.donkey.model.message.CapnpModel.CapMessage;
import com.mirth.connect.donkey.model.message.Message;
import com.mirth.connect.donkey.server.BdbJeDataSource;
import com.mirth.connect.donkey.server.Donkey;
import com.mirth.connect.model.filters.MessageFilter;
import com.mirth.connect.server.controllers.DonkeyMessageController.FilterOptions;
import com.mirth.connect.server.controllers.je.EvalResult;
import com.mirth.connect.server.mybatis.MessageSearchResult;
import com.mirth.connect.server.mybatis.MessageTextResult;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class MessageSelector {

    public static List<MessageTextResult> searchMessageTable(Transaction txn, MessageFilter filter, List<Field> nonNullFilterfields, Long localChannelId, FilterOptions filterOptions, long minMessageId, long maxMessageId) throws Exception {
        List<MessageTextResult> messageResults = new ArrayList<>();
        BdbJeDataSource ds = BdbJeDataSource.getInstance();
        Database messageDb = ds.getDbMap().get("d_m" + localChannelId);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Cursor cursor = messageDb.openCursor(txn, null);
        try {
            key.setData(longToBytes(minMessageId));
            OperationStatus os = cursor.getSearchKeyRange(key, data, null);
            if(os != OperationStatus.SUCCESS) {
                return messageResults;
            }
            // step back
            cursor.getPrev(key, data, null);
            
            Database atmtDb = ds.getDbMap().get("d_ma" + localChannelId);
            while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS) {
                CapMessage.Reader cr = readMessage(data.getData()).getRoot(CapMessage.factory);
                EvalResult er = evalMessage(txn, cr, atmtDb, nonNullFilterfields, filter, minMessageId, maxMessageId);
                if(er == EvalResult.SELECTED) {
                    MessageTextResult m = toMessageTextResult(cr, filter);
                    messageResults.add(m);
                }
                else if(er == EvalResult.NO_MORE) {
                    break;
                }
            }
        }
        finally {
            if(cursor != null) {
                cursor.close();
            }
        }
        return messageResults;
    }

    private static EvalResult evalMessage(Transaction txn, CapMessage.Reader cr, Database atmtDb, List<Field> fields, MessageFilter filter, long minMessageId, long maxMessageId) {
        boolean selected = false;
        EvalResult er = EvalResult.DROPPED;
        
        for(Field f : fields) {
            switch(f.getName()) {
            case "maxMessageId":
                if(cr.getMessageId() <= maxMessageId) {
                    selected = true;
                }
                else {
                    er = EvalResult.NO_MORE;
                }
                break;
            case "minMessageId":
                if(cr.getMessageId() >= minMessageId) {
                    selected = true;
                }
                else {
                    er = EvalResult.NO_MORE;
                }
                break;
            case "originalIdUpper":
                if(cr.getOriginalId() <= filter.getOriginalIdUpper()) {
                    selected = true;
                }
                else {
                    er = EvalResult.NO_MORE;
                }
                break;
            case "originalIdLower":
                if(cr.getOriginalId() >= filter.getOriginalIdLower()) {
                    selected = true;
                }
                else {
                    er = EvalResult.NO_MORE;
                }
                break;
            case "importIdUpper":
                if(cr.getImportId() <= filter.getImportIdUpper()) {
                    selected = true;
                }
                else {
                    er = EvalResult.NO_MORE;
                }
                break;
            case "importIdLower":
                if(cr.getImportId() >= filter.getImportIdLower()) {
                    selected = true;
                }
                else {
                    er = EvalResult.NO_MORE;
                }
                break;
            case "startDate":
                if(cr.getReceivedDate() >= filter.getStartDate().getTimeInMillis()) {
                    selected = true;
                }
                else {
                    er = EvalResult.NO_MORE;
                }
                break;
            case "endDate":
                if(cr.getReceivedDate() <= filter.getStartDate().getTimeInMillis()) {
                    selected = true;
                }
                else {
                    er = EvalResult.NO_MORE;
                }
                break;
            case "attachment":
                if(filter.getAttachment()) {
                    DatabaseEntry key = new DatabaseEntry(longToBytes(cr.getMessageId()));
                    DatabaseEntry data = new DatabaseEntry();
                    OperationStatus os = atmtDb.get(txn, key, data, LockMode.READ_COMMITTED);
                    selected = (os == OperationStatus.SUCCESS);
                }
                break;
            default:
                continue;
            }

            if(!selected) {
                break;
            }
        }
        
        if(selected) {
            er = EvalResult.SELECTED;
        }
        
        return er;
    }

    private static MessageTextResult toMessageTextResult(CapMessage.Reader cr, MessageFilter filter) {
        MessageTextResult mr = new MessageTextResult();
        mr.setMessageId(cr.getMessageId());
        if(cr.getImportId() != -1) {
            mr.setImportId(cr.getImportId());
        }
        mr.setProcessed(cr.getProcessed());

        return mr;
    }
    
    public static List<MessageSearchResult> selectMessagesById(Transaction txn, String serverId, long localChannelId, List<Long> includeMessageList, Long minMessageId, Long maxMessageId) throws Exception {
        List<MessageSearchResult> messageResults = new ArrayList<>();
        BdbJeDataSource ds = BdbJeDataSource.getInstance();
        Database messageDb = ds.getDbMap().get("d_m" + localChannelId);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Cursor cursor = messageDb.openCursor(txn, null);
        try {
            key.setData(longToBytes(minMessageId));
            OperationStatus os = cursor.getSearchKeyRange(key, data, null);
            if(os != OperationStatus.SUCCESS) {
                return messageResults;
            }
            // step back
            cursor.getPrev(key, data, null);
            
            while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS) {
                CapMessage.Reader cr = readMessage(data.getData()).getRoot(CapMessage.factory);
                EvalResult er = evalMessage(cr, includeMessageList, minMessageId, maxMessageId);
                if(er == EvalResult.SELECTED) {
                    MessageSearchResult m = toMessageSearchResult(cr, serverId);
                    messageResults.add(m);
                }
                else if(er == EvalResult.NO_MORE) {
                    break;
                }
            }
        }
        finally {
            if(cursor != null) {
                cursor.close();
            }
        }
        return messageResults;        
    }

    public static Message selectMessageById(Transaction txn, String serverId, long localChannelId, long messageId) throws Exception {
        Message m = null;
        BdbJeDataSource ds = BdbJeDataSource.getInstance();
        Database messageDb = ds.getDbMap().get("d_m" + localChannelId);

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        key.setData(longToBytes(messageId));
        OperationStatus os = messageDb.get(txn, key, data, LockMode.READ_COMMITTED);
        if(os == OperationStatus.SUCCESS) {
            CapMessage.Reader cr = readMessage(data.getData()).getRoot(CapMessage.factory);
            m = toMessage(cr, serverId);
        }

        return m;        
    }

    private static Message toMessage(CapMessage.Reader cr, String serverId) {
        Message msr = new Message();
        msr.setMessageId(cr.getMessageId());
        msr.setServerId(serverId);
        long rd = cr.getReceivedDate();
        if(rd != -1) {
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(rd);
            msr.setReceivedDate(c);
        }
        msr.setProcessed(cr.getProcessed());
        if(cr.getOriginalId() != -1) {
            msr.setOriginalId(cr.getOriginalId());
        }
        
        if(cr.hasImportChannelId()) {
            msr.setImportChannelId(cr.getImportChannelId().toString());
        }
        if(cr.getImportId() != -1) {
            msr.setImportId(cr.getImportId());
        }
        
        return msr;
    }

    private static MessageSearchResult toMessageSearchResult(CapMessage.Reader cr, String serverId) {
        MessageSearchResult msr = new MessageSearchResult();
        msr.setMessageId(cr.getMessageId());
        msr.setServerId(serverId);
        long rd = cr.getReceivedDate();
        if(rd != -1) {
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(rd);
            msr.setReceivedDate(c);
        }
        msr.setProcessed(cr.getProcessed());
        if(cr.getOriginalId() != -1) {
            msr.setOriginalId(cr.getOriginalId());
        }
        
        if(cr.hasImportChannelId()) {
            msr.setImportChannelId(cr.getImportChannelId().toString());
        }
        if(cr.getImportId() != -1) {
            msr.setImportId(cr.getImportId());
        }
        
        return msr;
    }

    private static EvalResult evalMessage(CapMessage.Reader cr, List<Long> includeMessageList, Long minMessageId, Long maxMessageId) {
        boolean selected = false;
        EvalResult er = EvalResult.DROPPED;
        
        long msgId = cr.getMessageId();
        if(minMessageId != null) {
            selected = (msgId >= minMessageId);
            if(!selected) {
                er = EvalResult.NO_MORE;
            }
        }
        
        if(maxMessageId != null) {
            selected = (msgId <= maxMessageId);
            if(!selected) {
                er = EvalResult.NO_MORE;
            }
        }
        
        if(includeMessageList != null) {
            selected = includeMessageList.contains(msgId);
        }
        
        if(selected) {
            er = EvalResult.SELECTED;
        }
        
        return er;
    }
}
