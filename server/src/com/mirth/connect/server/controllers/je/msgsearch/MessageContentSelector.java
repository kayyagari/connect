package com.mirth.connect.server.controllers.je.msgsearch;

import static com.mirth.connect.donkey.util.SerializerUtil.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent;
import com.mirth.connect.donkey.model.message.ContentType;
import com.mirth.connect.donkey.model.message.MessageContent;
import com.mirth.connect.donkey.server.BdbJeDataSource;
import com.mirth.connect.donkey.util.SerializerUtil;
import com.mirth.connect.model.filters.elements.ContentSearchElement;
import com.mirth.connect.server.controllers.je.EvalResult;
import com.mirth.connect.server.mybatis.MessageTextResult;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class MessageContentSelector {
    private static final ContentType[] cts = new ContentType[] {ContentType.RAW, ContentType.SOURCE_MAP};

    public static List<MessageTextResult> searchContentTable(Transaction txn, ContentSearchElement element, Integer metadataId, Integer contentType, long localChannelId, long minMessageId, long maxMessageId) throws Exception {
        List<MessageTextResult> contentResults = new ArrayList<>();
        BdbJeDataSource ds = BdbJeDataSource.getInstance();
        Database msgContentDb = ds.getDbMap().get("d_mc" + localChannelId);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Cursor cursor = msgContentDb.openCursor(txn, null);
        try {
            key.setData(longToBytes(minMessageId));
            OperationStatus os = cursor.getSearchKeyRange(key, data, null);
            if(os != OperationStatus.SUCCESS) {
                return contentResults;
            }
            // step back
            cursor.getPrev(key, data, null);
            
            while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS) {
                CapMessageContent.Reader cr = readMessage(data.getData()).getRoot(CapMessageContent.factory);
                EvalResult er = evalMessage(cr, element, metadataId, contentType, minMessageId, maxMessageId);
                if(er == EvalResult.SELECTED) {
                    MessageTextResult m = toMessageTextResult(cr);
                    contentResults.add(m);
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
        
        return contentResults;
    }
    
    private static MessageTextResult toMessageTextResult(CapMessageContent.Reader cr) {
        MessageTextResult mr = new MessageTextResult();
        mr.setMessageId(cr.getMessageId());
        mr.setMetaDataId(cr.getMetaDataId());
        int ct = SerializerUtil.fromCapContentType(cr.getContentType()).getContentTypeCode();
        mr.setContentType(ct);
        
        return mr;
    }

    private static EvalResult evalMessage(CapMessageContent.Reader cr, ContentSearchElement element, Integer metadataId, Integer contentType, long minMessageId, long maxMessageId) {
        EvalResult er = EvalResult.DROPPED;
        boolean selected = false;
        if(cr.getMessageId() >= minMessageId && cr.getMessageId() <= maxMessageId ) {
            selected = true;
        }
        else {
            er = EvalResult.NO_MORE;
        }
        
        if(selected && metadataId != null) {
            selected = (cr.getMetaDataId() == metadataId);
        }
        
        if(selected && contentType != null) {
            int ct = SerializerUtil.fromCapContentType(cr.getContentType()).getContentTypeCode();
            selected = (ct == contentType);
        }
        
        if(selected) {
            String content = cr.getContent().toString().toLowerCase();
            for(String s : element.getSearches()) {
                if(!StringUtils.contains(content, s.toLowerCase())) {
                    selected = false;
                    break;
                }
            }
        }
        
        if(selected) {
            er = EvalResult.SELECTED;
        }
        
        return er;
    }
    
    public static List<MessageContent> selectMessageForReprocessing(Transaction txn, long localChannelId, long messageId) throws Exception {
        BdbJeDataSource ds = BdbJeDataSource.getInstance();
        Database msgContentDb = ds.getDbMap().get("d_mc" + localChannelId);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        
        List<MessageContent> lst = new ArrayList<>();
        for(ContentType c : cts) {
            key.setData(buildPrimaryKeyOfMessageContent(messageId, 0, c));
            OperationStatus os = msgContentDb.get(txn, key, data, LockMode.READ_COMMITTED);
            if(os == OperationStatus.SUCCESS) {
                CapMessageContent.Reader cr = (CapMessageContent.Reader) readMessage(data.getData()).getRoot(CapMessageContent.factory);
                MessageContent mc = toMessageContent(cr);
                lst.add(mc);
            }
        }
        
        return lst;
    }

    private static MessageContent toMessageContent(CapMessageContent.Reader cr) {
        MessageContent mc = new MessageContent();
        mc.setMessageId(cr.getMessageId());
        mc.setMetaDataId(cr.getMetaDataId());
        mc.setContentType(fromCapContentType(cr.getContentType()));
        mc.setContent(cr.getContent().toString());
        mc.setDataType(cr.getDataType().toString());
        mc.setEncrypted(cr.getEncrypted());
        
        return mc;
    }
}
