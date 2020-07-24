package com.mirth.connect.server.controllers.je.msgsearch;

import static com.mirth.connect.donkey.util.SerializerUtil.longToBytes;
import static com.mirth.connect.donkey.util.SerializerUtil.readMessage;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.mirth.connect.donkey.model.message.CapnpModel.CapMetadata;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMetadataColumn;
import com.mirth.connect.donkey.server.BdbJeDataSource;
import com.mirth.connect.model.filters.MessageFilter;
import com.mirth.connect.model.filters.elements.MetaDataSearchElement;
import com.mirth.connect.model.filters.elements.MetaDataSearchOperator;
import com.mirth.connect.server.controllers.je.EvalResult;
import com.mirth.connect.server.mybatis.MessageTextResult;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class CustomeMetadataSelector {
    public static List<MessageTextResult> searchCustomMetaDataTable(Transaction txn, MessageFilter filter, List<Field> nonNullFilterfields, Long localChannelId, long minMessageId, long maxMessageId) throws Exception {
        List<MessageTextResult> metaDataResults = new ArrayList<>();
        BdbJeDataSource ds = BdbJeDataSource.getInstance();
        Database metadataDb = ds.getDbMap().get("d_mcm" + localChannelId);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Cursor cursor = metadataDb.openCursor(txn, null);
        try {
            key.setData(longToBytes(minMessageId));
            OperationStatus os = cursor.getSearchKeyRange(key, data, null);
            if(os != OperationStatus.SUCCESS) {
                return metaDataResults;
            }
            // step back
            cursor.getPrev(key, data, null);
            
            while(cursor.getSearchKeyRange(key, data, null) == OperationStatus.SUCCESS) {
                CapMetadata.Reader cr = readMessage(data.getData()).getRoot(CapMetadata.factory);
                EvalResult er = evalCustomeMetadata(txn, cr, nonNullFilterfields, filter, minMessageId, maxMessageId);
                if(er == EvalResult.SELECTED) {
                    MessageTextResult m = toMessageTextResult(cr, filter);
                    metaDataResults.add(m);
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
        
        return metaDataResults;
    }
    
    private static MessageTextResult toMessageTextResult(CapMetadata.Reader cr, MessageFilter filter) {
        MessageTextResult mtr = new MessageTextResult();
        mtr.setMessageId(cr.getMessageId());
        mtr.setMetaDataId(cr.getMetadataId());
        return mtr;
    }

    private static EvalResult evalCustomeMetadata(Transaction txn, CapMetadata.Reader cr, List<Field> fields, MessageFilter filter, long minMessageId, long maxMessageId) {
        boolean selected = false;
        EvalResult er = EvalResult.DROPPED;

        Map<String, String> metaDataMap = null;

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
            case "includedMetaDataIds":
                selected = filter.getIncludedMetaDataIds().contains(cr.getMetadataId());
                break;
            case "excludedMetaDataIds":
                selected = !filter.getExcludedMetaDataIds().contains(cr.getMetadataId());
                break;
            case "textSearch":
                if(metaDataMap == null) {
                    metaDataMap = readMetadataMap(cr);
                }
                String ts = filter.getTextSearch();
                List<String> columns = filter.getTextSearchMetaDataColumns();
                if(columns != null) {
                    for(String s : columns) {
                        String val = metaDataMap.get(s.toUpperCase());
                        if(StringUtils.containsIgnoreCase(val, ts)) { //not the column name but fetch the value of column and compare
                            selected = true;
                        }
                    }
                }
                break;
            case "metaDataSearch":
                if(metaDataMap == null) {
                    metaDataMap = readMetadataMap(cr);
                }
                selected = searchMetadataMap(metaDataMap, filter.getMetaDataSearch());
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

    private static boolean searchMetadataMap(Map<String,String> metadataMap, List<MetaDataSearchElement> lst) {
        boolean selected = false;
        for(MetaDataSearchElement mse : lst) {
            String colVal = metadataMap.get(mse.getColumnName().toUpperCase());
            if(colVal == null) {
                selected = false;
                break;
            }
            String inputVal = String.valueOf(mse.getValue());
            if(mse.getIgnoreCase() != null && mse.getIgnoreCase()) {
                colVal = colVal.toLowerCase();
                inputVal = inputVal.toLowerCase();
            }

            MetaDataSearchOperator op = MetaDataSearchOperator.fromString(mse.getOperator());
            switch(op) {
            case CONTAINS:
                selected = StringUtils.contains(colVal, inputVal);
                break;
            case DOES_NOT_CONTAIN:
                selected = !StringUtils.contains(colVal, inputVal);
                break;
            case DOES_NOT_END_WITH:
                selected = !StringUtils.endsWith(colVal, inputVal);
                break;
            case DOES_NOT_START_WITH:
                selected = !StringUtils.startsWith(colVal, inputVal);
                break;
            case ENDS_WITH:
                selected = StringUtils.endsWith(colVal, inputVal);
                break;
            case EQUAL:
                selected = StringUtils.equals(colVal, inputVal);
                break;
            case GREATER_THAN:
                selected = colVal.compareTo(inputVal) > 0;
                break;
            case GREATER_THAN_OR_EQUAL:
                selected = colVal.compareTo(inputVal) >= 0;
                break;
            case LESS_THAN:
                selected = colVal.compareTo(inputVal) < 0;
                break;
            case LESS_THAN_OR_EQUAL:
                selected = colVal.compareTo(inputVal) <= 0;
                break;
            case NOT_EQUAL:
                selected = !StringUtils.equals(colVal, inputVal);
                break;
            case STARTS_WITH:
                selected = StringUtils.startsWith(colVal, inputVal);
                break;
            }
            
            if(!selected) {
                break;
            }
        }
        
        return selected;
    }

    private static Map<String, String> readMetadataMap(CapMetadata.Reader cm) {
        Map<String, String> metaDataMap = new HashMap<>();
        if(cm.hasColumns()) {
            for(CapMetadataColumn.Reader c : cm.getColumns()) {
                String name = c.getName().toString();
                String storedVal = c.getValue().toString();
                metaDataMap.put(name, storedVal);
            }
        }
        return metaDataMap;
    }
}
