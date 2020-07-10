package com.mirth.connect.server.controllers.je;

import com.mirth.connect.model.Cacheable;
import com.mirth.connect.model.converters.ObjectXMLSerializer;
import com.mirth.connect.server.controllers.Cache;
import com.sleepycat.je.Database;
import com.sleepycat.je.Environment;

public class BdbJeObjectCache<V extends Cacheable<V>> extends Cache<V> {
    protected Database db;
    protected Environment env;
    protected ObjectXMLSerializer objectSerializer;

    public BdbJeObjectCache(Environment env, Database db) {
        this(env, db, true);
    }
    
    public BdbJeObjectCache(Environment env, Database db, boolean nameUnique) {
        super(db.getDatabaseName(), "", "", nameUnique);
        this.db = db;
        this.env = env;
        objectSerializer = ObjectXMLSerializer.getInstance();
    }
}
