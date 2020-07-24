package com.mirth.connect.server.controllers.je.msgsearch;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import com.mirth.connect.donkey.model.message.CapnpModel.CapConnectorMessage;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMessage;
import com.mirth.connect.donkey.model.message.Message;
import com.mirth.connect.donkey.server.BdbJeDataSource;
import static com.mirth.connect.donkey.util.SerializerUtil.*;
import com.mirth.connect.model.filters.EventFilter;
import com.mirth.connect.model.filters.MessageFilter;
import com.mirth.connect.server.controllers.DonkeyMessageController.FilterOptions;
import com.mirth.connect.server.controllers.je.EvalResult;
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
            while(cursor.getSearchKeyRange(key, data, null) == OperationStatus.SUCCESS) {
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
        return mr;
    }
}
