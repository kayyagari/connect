package com.mirth.connect.server.controllers.je.msgsearch;

import static com.mirth.connect.donkey.util.SerializerUtil.*;
import static com.mirth.connect.donkey.util.SerializerUtil.readMessage;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.mirth.connect.donkey.model.message.CapnpModel.CapConnectorMessage;
import com.mirth.connect.donkey.model.message.Status;
import com.mirth.connect.donkey.server.BdbJeDataSource;
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

public class MetadataSelector {
    public static List<MessageTextResult> searchMetaDataTable(Transaction txn, MessageFilter filter, List<Field> nonNullFilterfields, Long localChannelId, FilterOptions filterOptions, long minMessageId, long maxMessageId) throws Exception {
        List<MessageTextResult> metaDataResults = new ArrayList<>();
        BdbJeDataSource ds = BdbJeDataSource.getInstance();
        Database conMsgDb = ds.getDbMap().get("d_mm" + localChannelId);
        Database conMsgStatusDb = ds.getDbMap().get("d_mm_status" + localChannelId);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Cursor cursor = conMsgDb.openCursor(txn, null);
        try {
            key.setData(longToBytes(minMessageId));
            OperationStatus os = cursor.getSearchKeyRange(key, data, null);
            if(os != OperationStatus.SUCCESS) {
                return metaDataResults;
            }
            
            do {
                CapConnectorMessage.Reader cr = readMessage(data.getData()).getRoot(CapConnectorMessage.factory);
                EvalResult er = evalConnectorMessage(txn, cr, conMsgStatusDb, nonNullFilterfields, filter, minMessageId, maxMessageId);
                if(er == EvalResult.SELECTED) {
                    MessageTextResult m = toMessageTextResult(cr, filter);
                    metaDataResults.add(m);
                }
                else if(er == EvalResult.NO_MORE) {
                    break;
                }
            }
            while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS);
        }
        finally {
            if(cursor != null) {
                cursor.close();
            }
        }
        
        return metaDataResults;
    }

    private static MessageTextResult toMessageTextResult(CapConnectorMessage.Reader cr, MessageFilter filter) {
        MessageTextResult mtr = new MessageTextResult();
        mtr.setMessageId(cr.getMessageId());
        mtr.setMetaDataId(cr.getId());
        if(filter.getTextSearch() != null) {
            boolean textFound = StringUtils.containsIgnoreCase(cr.getConnectorName().toString(), filter.getTextSearch());
            mtr.setTextFound(textFound);
        }
        
        return mtr;
    }

    private static EvalResult evalConnectorMessage(Transaction txn, CapConnectorMessage.Reader cr, Database conMsgStatusDb, List<Field> fields, MessageFilter filter, long minMessageId, long maxMessageId) {
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
            case "statuses":
                DatabaseEntry key = new  DatabaseEntry(buildPrimaryKeyOfConnectorMessage(cr.getMessageId(), cr.getId()));
                DatabaseEntry data = new  DatabaseEntry();
                OperationStatus os = conMsgStatusDb.get(txn, key, data, LockMode.READ_COMMITTED);
                if(os == OperationStatus.SUCCESS) {
                    Status s = Status.fromChar((char)data.getData()[0]);
                    selected = filter.getStatuses().contains(s);
                }
                break;
            case "includedMetaDataIds":
                selected = filter.getIncludedMetaDataIds().contains(cr.getId());
                break;
            case "excludedMetaDataIds":
                selected = !filter.getExcludedMetaDataIds().contains(cr.getId());
                break;
            case "sendAttemptsLower":
                if(cr.getSendAttempts() >= filter.getSendAttemptsLower()) {
                    selected = true;
                }
                break;
            case "sendAttemptsUpper":
                if(cr.getSendAttempts() <= filter.getSendAttemptsUpper()) {
                    selected = true;
                }
                break;
            case "error":
                if(filter.getError()) {
                    selected = cr.getErrorCode() > 0;
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
}
