package com.mirth.connect.server.controllers.je;

import org.apache.log4j.Logger;

import com.mirth.connect.model.converters.ObjectXMLSerializer;
import com.mirth.connect.server.controllers.Cache;
import com.sleepycat.je.Database;
import com.sleepycat.je.Environment;

public class BdbJeObjectCache<V> extends Cache {
    protected Database db;
    protected Environment env;
    protected ObjectXMLSerializer objectSerializer;

    private static Logger logger = Logger.getLogger(BdbJeObjectCache.class);

    public BdbJeObjectCache(Environment env, Database db) {
        super(db.getDatabaseName(), "", "");
        this.db = db;
        this.env = env;
        objectSerializer = ObjectXMLSerializer.getInstance();
    }
}
