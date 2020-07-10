package com.mirth.connect.server.controllers.je;

import static com.mirth.connect.donkey.util.SerializerUtil.readMessage;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.mirth.connect.donkey.model.message.CapnpModel.CapChannel;
import com.mirth.connect.model.codetemplates.CodeTemplate;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class BdbJeCodeTemplateLibraryCache extends BdbJeObjectCache<CodeTemplate> {
    private static Logger logger = Logger.getLogger(BdbJeCodeTemplateLibraryCache.class);

    public BdbJeCodeTemplateLibraryCache(Environment env, Database db) {
        super(env, db);
    }
    
    @Override
    protected synchronized void refreshCache() {
        Transaction txn = null;
        Cursor cursor = null;
        try {
            Map<String, Integer> databaseRevisions = new HashMap<>();

            txn = env.beginTransaction(null, null);
            cursor = db.openCursor(txn, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS) {
                CapChannel.Reader cr = readMessage(data.getData()).getRoot(CapChannel.factory);
                String id = cr.getId().toString();
                int revision = cr.getRevision();
                databaseRevisions.put(id, revision);

                if (!cacheById.containsKey(id) || revision > cacheById.get(id).getRevision()) {
                    String name = cr.getName().toString();
                    CodeTemplate oldItem = cacheById.get(id);
                    CodeTemplate item = objectSerializer.deserialize(cr.getChannel().toString(), CodeTemplate.class);
                    cacheById.put(id, item);
                    
                    if (isNameUnique()) {
                        cacheByName.put(name, item);
                        
                        // If the name changed, remove the old name from the cache
                        if (oldItem != null) {
                            String oldName = oldItem.getName();
                            if (!oldName.equals(name)) {
                                cacheByName.remove(oldName);
                            }
                        }
                    }
                }
            }
            cursor.close();

            // Remove any from the cache that no longer exist in the database
            for (String id : cacheById.keySet()) {
                if (!databaseRevisions.containsKey(id)) {
                    // Remove from cache
                    CodeTemplate item = cacheById.remove(id);
                    if (isNameUnique()) {
                        cacheByName.remove(item.getName());
                    }
                }
            }
            txn.commit();
        }
        catch (Exception e) {
            if(cursor != null) {
                cursor.close();
            }
            if(txn != null) {
                txn.abort();
            }
            logger.error("Error refreshing CodeTemplate cache", e);
        }
    }
}
