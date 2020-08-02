package com.mirth.connect.server.controllers.je;

import static com.mirth.connect.donkey.util.SerializerUtil.readMessage;
import static com.mirth.connect.donkey.util.SerializerUtil.writeMessageToEntry;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

import com.mirth.connect.client.core.ControllerException;
import com.mirth.connect.donkey.model.message.CapnpModel.CapScript;
import com.mirth.connect.donkey.server.BdbJeDataSource;
import com.mirth.connect.donkey.server.data.jdbc.ReusableMessageBuilder;
import com.mirth.connect.server.ExtensionLoader;
import com.mirth.connect.server.builders.JavaScriptBuilder;
import com.mirth.connect.server.controllers.DefaultScriptController;
import com.mirth.connect.server.controllers.ScriptController;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class BdbJeScriptController extends DefaultScriptController {
    private Environment env;
    private Database db;
    private GenericKeyedObjectPool<Class, ReusableMessageBuilder> serverObjectPool;

    private static final byte DELIM_BYTE = '\\';

    public static ScriptController create() {
        synchronized (BdbJeScriptController.class) {
            if (instance == null) {
                instance = ExtensionLoader.getInstance().getControllerInstance(ScriptController.class);

                if (instance == null) {
                    BdbJeDataSource ds = BdbJeDataSource.getInstance();
                    BdbJeScriptController i = new BdbJeScriptController();
                    i.env = ds.getBdbJeEnv();
                    String name = "script";
                    i.db = ds.getDbMap().get(name);
                    i.serverObjectPool = ds.getServerObjectPool();
                    instance = i;
                }
            }

            return instance;
        }
    }

    @Override
    public void putScript(String groupId, String id, String script) throws ControllerException {
        Transaction txn = null;
        try {
            txn = env.beginTransaction(null, null);
            _putScript(txn, groupId, id, script);
            txn.commit();
        }
        catch(ControllerException e) {
            txn.abort();
            throw e;
        }
    }

    private void _putScript(Transaction txn, String groupId, String id, String script) throws ControllerException {
        ReusableMessageBuilder rmb = null;
        try {
            rmb = serverObjectPool.borrowObject(CapScript.class);
            CapScript.Builder cb = (CapScript.Builder)rmb.getSb();
            cb.setGroupId(groupId);
            cb.setId(id);
            cb.setScript(script);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            key.setData(buildScriptKey(groupId, id));
            writeMessageToEntry(rmb, data);
            
            db.put(txn, key, data);
        }
        catch(Exception e) {
            throw new ControllerException(e);
        }
        finally {
            if(rmb != null) {
                serverObjectPool.returnObject(CapScript.class, rmb);
            }
        }
    }

    @Override
    public String getScript(String groupId, String id) throws ControllerException {
        Transaction txn = null;
        String script = null;
        try {
            txn = env.beginTransaction(null, null);
            script = _getScript(txn, groupId, id);
            txn.commit();
        }
        catch(ControllerException e) {
            txn.abort();
            throw e;
        }
        
        return script;
    }

    private String _getScript(Transaction txn, String groupId, String id) throws ControllerException {
        String script = null;
        try {
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            key.setData(buildScriptKey(groupId, id));
            OperationStatus os = db.get(txn, key, data, LockMode.READ_COMMITTED);
            if(os == OperationStatus.SUCCESS) {
                CapScript.Reader cr = (CapScript.Reader)readMessage(data.getData()).getRoot(CapScript.factory);
                script = cr.getScript().toString();
            }
        }
        catch(Exception e) {
            throw new ControllerException(e);
        }
        
        return script;
    }

    @Override
    public void removeScripts(String groupId) throws ControllerException {
        Transaction txn = null;
        Cursor cursor = null;
        try {
            txn= env.beginTransaction(null, null);
            cursor = db.openCursor(txn, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            byte[] prefix = buildScriptKeyPrefix(groupId);
            key.setData(prefix);

            OperationStatus os = cursor.getSearchKeyRange(key, data, null);
            if(os == OperationStatus.SUCCESS) {
                // step back
                cursor.getPrev(key, data, null);
            }

            outer: while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS) {
                byte[] tmp = key.getData();
                for(int i=0; i< prefix.length; i++) {
                    if(prefix[i] != tmp[i]) {
                        break outer;
                    }
                }
                cursor.delete();
            }
            cursor.close();
            txn.commit();
        }
        catch(Exception e) {
            if(cursor != null) {
                cursor.close();
            }
            txn.abort();
            throw new ControllerException(e);
        }
    }

    @Override
    public void vacuumScriptTable() {
    }

    @Override
    public Map<String, String> getGlobalScripts() throws ControllerException {
        Map<String, String> scripts = new HashMap<String, String>();

        Transaction txn = env.beginTransaction(null, null);
        try {
            String deployScript = _getScript(txn, GLOBAL_GROUP_ID, DEPLOY_SCRIPT_KEY);
            String undeployScript = _getScript(txn, GLOBAL_GROUP_ID, UNDEPLOY_SCRIPT_KEY);
            String preprocessorScript = _getScript(txn, GLOBAL_GROUP_ID, PREPROCESSOR_SCRIPT_KEY);
            String postprocessorScript = _getScript(txn, GLOBAL_GROUP_ID, POSTPROCESSOR_SCRIPT_KEY);
            
            if (StringUtils.isBlank(deployScript)) {
                deployScript = JavaScriptBuilder.generateDefaultKeyScript(DEPLOY_SCRIPT_KEY);
            }
            
            if (StringUtils.isBlank(undeployScript)) {
                undeployScript = JavaScriptBuilder.generateDefaultKeyScript(UNDEPLOY_SCRIPT_KEY);
            }
            
            if (StringUtils.isBlank(preprocessorScript)) {
                preprocessorScript = JavaScriptBuilder.generateDefaultKeyScript(PREPROCESSOR_SCRIPT_KEY);
            }
            
            if (StringUtils.isBlank(postprocessorScript)) {
                postprocessorScript = JavaScriptBuilder.generateDefaultKeyScript(POSTPROCESSOR_SCRIPT_KEY);
            }
            
            scripts.put(DEPLOY_SCRIPT_KEY, deployScript);
            scripts.put(UNDEPLOY_SCRIPT_KEY, undeployScript);
            scripts.put(PREPROCESSOR_SCRIPT_KEY, preprocessorScript);
            scripts.put(POSTPROCESSOR_SCRIPT_KEY, postprocessorScript);
        }
        finally {
            txn.commit();
        }

        return scripts;
    }

    @Override
    protected void putGlobalScriptsToDB(Map<String, String> scripts) throws ControllerException {
        Transaction txn = env.beginTransaction(null, null);
        try {
            for (Entry<String, String> entry : scripts.entrySet()) {
                _putScript(txn, GLOBAL_GROUP_ID, entry.getKey(), entry.getValue());
            }
            txn.commit();
        }
        catch(ControllerException e) {
            txn.abort();
            throw e;
        }
    }

    private byte[] buildScriptKey(String groupId, String id) {
        byte[] g = groupId.getBytes(utf8);
        byte[] i = id.getBytes(utf8);
        byte[] data = new byte[g.length + 1 + i.length];
        System.arraycopy(g, 0, data, 0, g.length);
        data[g.length] = DELIM_BYTE;
        System.arraycopy(i, 0, data, g.length+1, i.length);
        
        return data;
    }

    private byte[] buildScriptKeyPrefix(String groupId) {
        byte[] g = groupId.getBytes(utf8);
        byte[] data = new byte[g.length + 1];
        System.arraycopy(g, 0, data, 0, g.length);
        data[g.length] = DELIM_BYTE;
        
        return data;
    }
}
