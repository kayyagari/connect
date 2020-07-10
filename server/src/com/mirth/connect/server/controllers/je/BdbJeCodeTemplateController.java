package com.mirth.connect.server.controllers.je;

import static com.mirth.connect.donkey.util.SerializerUtil.writeMessageToEntry;

import java.util.Calendar;
import java.util.List;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.log4j.Logger;

import com.mirth.connect.client.core.ControllerException;
import com.mirth.connect.donkey.model.message.CapnpModel.CapCodeTemplate;
import com.mirth.connect.donkey.model.message.CapnpModel.CapCodeTemplateLibrary;
import com.mirth.connect.donkey.server.BdbJeDataSource;
import com.mirth.connect.donkey.server.data.jdbc.ReusableMessageBuilder;
import com.mirth.connect.model.ServerEventContext;
import com.mirth.connect.model.codetemplates.CodeTemplate;
import com.mirth.connect.model.codetemplates.CodeTemplateLibrary;
import com.mirth.connect.model.codetemplates.CodeTemplateLibrarySaveResult;
import com.mirth.connect.model.codetemplates.CodeTemplateLibrarySaveResult.CodeTemplateUpdateResult;
import com.mirth.connect.model.converters.ObjectXMLSerializer;
import com.mirth.connect.plugins.CodeTemplateServerPlugin;
import com.mirth.connect.server.ExtensionLoader;
import com.mirth.connect.server.controllers.CodeTemplateController;
import com.mirth.connect.server.controllers.DefaultCodeTemplateController;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;

public class BdbJeCodeTemplateController extends DefaultCodeTemplateController {
    private Environment env;
    private Database codeTmplDb;
    private Database libDb;
    private GenericKeyedObjectPool<Class, ReusableMessageBuilder> serverObjectPool;
    private ObjectXMLSerializer objectSerializer;
    
    private Logger logger = Logger.getLogger(BdbJeCodeTemplateController.class);

    protected BdbJeCodeTemplateController() {
    }
    
    public static CodeTemplateController create() {
        synchronized (BdbJeCodeTemplateController.class) {
            if (instance == null) {
                instance = ExtensionLoader.getInstance().getControllerInstance(CodeTemplateController.class);

                if (instance == null) {
                    BdbJeCodeTemplateController i = new BdbJeCodeTemplateController();
                    BdbJeDataSource ds = BdbJeDataSource.getInstance();
                    i.env = ds.getBdbJeEnv();
                    i.codeTmplDb = ds.getDbMap().get("code_template");
                    i.libDb = ds.getDbMap().get("code_template_library");
                    i.serverObjectPool = ds.getServerObjectPool();
                    i.objectSerializer = ObjectXMLSerializer.getInstance();
                    i.libraryCache = new BdbJeCodeTemplateCache(i.env, i.codeTmplDb);
                    i.codeTemplateCache = new BdbJeCodeTemplateLibraryCache(i.env, i.libDb);
                    instance = i;
                }
            }

            return instance;
        }
    }

    @Override
    protected void updateLibraries_db(Transaction txn, List<CodeTemplateLibrary> librariesToRemove, ServerEventContext context, List<CodeTemplateLibrary> libraries,
            Set<String> unchangedLibraryIds) throws ControllerException {
        try {
            // Remove libraries
            for (CodeTemplateLibrary library : librariesToRemove) {
                libDb.delete(txn, new DatabaseEntry(library.getId().getBytes(utf8)));
                // Invoke the code template plugins
                for (CodeTemplateServerPlugin codeTemplateServerPlugin : extensionController.getCodeTemplateServerPlugins().values()) {
                    codeTemplateServerPlugin.remove(library, context);
                }
            }

            // Insert or update libraries
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            for (CodeTemplateLibrary library : libraries) {
                // Only if it actually changed
                if (!unchangedLibraryIds.contains(library.getId())) {
                    library.setLastModified(Calendar.getInstance());

                    ReusableMessageBuilder rmb = serverObjectPool.borrowObject(CapCodeTemplateLibrary.class);
                    CapCodeTemplateLibrary.Builder ctlb = (CapCodeTemplateLibrary.Builder) rmb.getSb();
                    ctlb.setId(library.getId());
                    ctlb.setLibrary(objectSerializer.serialize(library));
                    ctlb.setName(library.getName());
                    ctlb.setRevision(library.getRevision());

                    key.setData(library.getId().getBytes(utf8));
                    writeMessageToEntry(rmb, data);
                    
                    // Put the new library in the database
                    logger.debug("Inserting code template library");
                    libDb.put(txn, key, data);
                    serverObjectPool.returnObject(CapCodeTemplateLibrary.class, rmb);

                    // Invoke the code template plugins
                    for (CodeTemplateServerPlugin codeTemplateServerPlugin : extensionController.getCodeTemplateServerPlugins().values()) {
                        codeTemplateServerPlugin.save(library, context);
                    }
                }
            }
        }
        catch(Exception e) {
            throw new ControllerException(e);
        }
    }

    @Override
    protected void removeCodeTemplate_db(Transaction txn, CodeTemplate codeTemplate) throws ControllerException {
        try {
            DatabaseEntry key = new DatabaseEntry(codeTemplate.getId().getBytes(utf8));
            codeTmplDb.delete(txn, key);
        }
        catch(Exception e) {
            throw new ControllerException(e);
        }
    }

    @Override
    protected void updateCodeTemplate_db(Transaction txn, CodeTemplate codeTemplate) throws ControllerException {
        ReusableMessageBuilder rmb = null;
        try {
            rmb = serverObjectPool.borrowObject(CapCodeTemplate.class);
            CapCodeTemplate.Builder cctb = (CapCodeTemplate.Builder) rmb.getSb();
            cctb.setCodeTemplate(objectSerializer.serialize(codeTemplate));
            cctb.setId(codeTemplate.getId());
            cctb.setName(codeTemplate.getName());
            cctb.setRevision(codeTemplate.getRevision());
            DatabaseEntry key = new DatabaseEntry(codeTemplate.getId().getBytes(utf8));
            DatabaseEntry data = new DatabaseEntry();
            writeMessageToEntry(rmb, data);
            codeTmplDb.put(txn, key, data);
        }
        catch(Exception e) {
            throw new ControllerException(e);
        }
        finally {
            if(rmb != null) {
                serverObjectPool.returnObject(CapCodeTemplate.class, rmb);
            }
        }
    }

    @Override
    public synchronized boolean updateLibraries(List<CodeTemplateLibrary> libraries, ServerEventContext context, boolean override) throws ControllerException {
        Transaction txn = null;
        boolean result = false;
        try {
            txn = env.beginTransaction(null, null);
            result = updateLibraries(txn, libraries, context, override);
            txn.commit();
        }
        catch(ControllerException e) {
            txn.abort();
            throw e;
        }
        return result;
    }

    @Override
    public synchronized boolean updateCodeTemplate(CodeTemplate codeTemplate,ServerEventContext context, boolean override) throws ControllerException {
        Transaction txn = null;
        boolean result = false;
        try {
            txn = env.beginTransaction(null, null);
            result = updateCodeTemplate(txn, codeTemplate, context, override);
            txn.commit();
        }
        catch(ControllerException e) {
            txn.abort();
            throw e;
        }
        return result;
    }

    @Override
    public synchronized void removeCodeTemplate(String codeTemplateId, ServerEventContext context) throws ControllerException {
        Transaction txn = null;
        try {
            txn = env.beginTransaction(null, null);
            removeCodeTemplate(txn, codeTemplateId, context);
            txn.commit();
        }
        catch(ControllerException e) {
            txn.abort();
            throw e;
        }
    }

    @Override
    public synchronized CodeTemplateLibrarySaveResult updateLibrariesAndTemplates(List<CodeTemplateLibrary> libraries, Set<String> removedLibraryIds, List<CodeTemplate> updatedCodeTemplates, Set<String> removedCodeTemplateIds, ServerEventContext context, boolean override) {
        CodeTemplateLibrarySaveResult result = null;
        Transaction txn = env.beginTransaction(null, null);
        result = updateLibrariesAndTemplates(txn, libraries, removedLibraryIds, updatedCodeTemplates, removedCodeTemplateIds, context, override);
        boolean success = result.isLibrariesSuccess();
        if(success) {
            for (CodeTemplateUpdateResult r : result.getCodeTemplateResults().values()) {
                if(!r.isSuccess()) {
                    success = false;
                    break;
                }
            }
        }
        
        if(success) {
           txn.commit(); 
        }
        else {
            txn.abort();
        }

        return result;
    }

    @Override
    public void vacuumLibraryTable() {
    }
    
    @Override
    public void vacuumCodeTemplateTable() {
    }
}
