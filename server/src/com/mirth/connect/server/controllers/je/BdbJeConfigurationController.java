package com.mirth.connect.server.controllers.je;

import static com.mirth.connect.donkey.util.SerializerUtil.deserializeProps;
import static com.mirth.connect.donkey.util.SerializerUtil.readMessage;
import static com.mirth.connect.donkey.util.SerializerUtil.serializeProps;
import static com.mirth.connect.donkey.util.SerializerUtil.writeMessageToEntry;

import java.io.InputStream;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.log4j.Logger;

import com.mirth.connect.donkey.model.message.CapnpModel.CapConfiguration;
import com.mirth.connect.donkey.server.BdbJeDataSource;
import com.mirth.connect.donkey.server.data.jdbc.ReusableMessageBuilder;
import com.mirth.connect.server.ExtensionLoader;
import com.mirth.connect.server.controllers.ConfigurationController;
import com.mirth.connect.server.controllers.ControllerFactory;
import com.mirth.connect.server.controllers.DefaultConfigurationController;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class BdbJeConfigurationController extends DefaultConfigurationController {
    private Environment env;
    private Database db;
    private GenericKeyedObjectPool<Class, ReusableMessageBuilder> serverObjectPool;
    
    private Logger logger = Logger.getLogger(BdbJeConfigurationController.class);
    
    protected BdbJeConfigurationController() {}

    public static ConfigurationController create() {
        synchronized (BdbJeConfigurationController.class) {
            if (instance == null) {
                instance = ExtensionLoader.getInstance().getControllerInstance(ConfigurationController.class);

                if (instance == null) {
                    BdbJeDataSource ds = BdbJeDataSource.getInstance();
                    BdbJeConfigurationController i = new BdbJeConfigurationController();
                    i.env = ds.getBdbJeEnv();
                    String name = "configuration";
                    i.db = ds.getDbMap().get(name);
                    i.serverObjectPool = ds.getServerObjectPool();
                    i.initialize();
                    instance = i;
                }
            }

            return instance;
        }
    }

    @Override
    public int getStatus(boolean checkDatabase) {
        logger.debug("getting Mirth status");

        // If the engine isn't running (only if it isn't starting) return STATUS_UNAVAILABLE.
        if (!ControllerFactory.getFactory().createEngineController().isRunning() && status != STATUS_ENGINE_STARTING) {
            return STATUS_UNAVAILABLE;
        }

        return status;
    }

    @Override
    public Properties getPropertiesForGroup(String category, Set<String> propertyKeys) {
        Function<Properties, Properties> f = new Function<Properties, Properties>() {
            @Override
            public Properties apply(Properties in) {
                if(propertyKeys == null || propertyKeys.isEmpty()) {
                    return in;
                }
                else {
                    Properties props = new Properties();
                    for(String k : propertyKeys) {
                        props.put(k, in.get(k));
                    }
                    return props;
                }
            }
        };
        
        return execFunction(category, f, false);
    }

    @Override
    public void removePropertiesForGroup(String category) {
        logger.debug("deleting all properties: category=" + category);
        Transaction txn = null;
        try {
            txn = env.beginTransaction(null, null);
            DatabaseEntry key = new DatabaseEntry(category.getBytes(utf8));
            db.delete(txn, key);
        }
        catch(Exception e) {
            if(txn != null) {
                txn.abort();
            }
            logger.error("Could not delete properties: category=" + category);
        }
    }

    @Override
    public String getProperty(String category, String name) {
        Function<Properties, String> f = new Function<Properties, String>() {
            @Override
            public String apply(Properties p) {
                String val = null;
                if(p != null) {
                    val = (String) p.get(name);
                }
                return val;
            }
        };

        return execFunction(category, f, false);
    }

    @Override
    public void saveProperty(String category, String name, String value) {
        ReusableMessageBuilder rmb = null;
        Transaction txn = null;
        try {
            txn = env.beginTransaction(null, null);
            DatabaseEntry key = new DatabaseEntry(category.getBytes(utf8));
            DatabaseEntry data = new DatabaseEntry();
            OperationStatus os = db.get(txn, key, data, LockMode.READ_COMMITTED);
            rmb = serverObjectPool.borrowObject(CapConfiguration.class);
            Properties props = new Properties();
            if(os == OperationStatus.SUCCESS) {
                CapConfiguration.Reader cr = readMessage(data.getData()).getRoot(CapConfiguration.factory);
                InputStream in = new ArrayInputStream(cr.getProps().toArray());
                props.load(in);
                rmb.prepareForUpdate(cr);
            }
            
            CapConfiguration.Builder cb = (CapConfiguration.Builder) rmb.getSb();
            props.put(name, value);
            cb.setProps(serializeProps(props));
            cb.setCategory(category);
            writeMessageToEntry(rmb, data);
            db.put(txn, key, data);
            txn.commit();
        }
        catch(Exception e) {
            if(txn != null) {
                txn.abort();
            }
        }
        finally {
            if(rmb != null) {
                serverObjectPool.returnObject(CapConfiguration.class, rmb);
            }
        }
    }

    @Override
    public void vacuumConfigurationTable() {
        // not needed
    }

    @Override
    public void removeProperty(String category, String name) {
        Function<Properties, Void> f = new Function<Properties, Void>() {
            @Override
            public Void apply(Properties p) {
                p.remove(name);
                return null;
            }
        };

        execFunction(category, f, true);
    }

    private <R> R execFunction(String category, Function<Properties, R> f, boolean write) {
        ReusableMessageBuilder rmb = null;
        Transaction txn = null;
        R val = null;
        try {
            txn = env.beginTransaction(null, null);
            DatabaseEntry key = new DatabaseEntry(category.getBytes(utf8));
            DatabaseEntry data = new DatabaseEntry();
            OperationStatus os = db.get(txn, key, data, LockMode.READ_COMMITTED);
            rmb = serverObjectPool.borrowObject(CapConfiguration.class);
            Properties props = new Properties();
            if(os == OperationStatus.SUCCESS) {
                CapConfiguration.Reader cr = readMessage(data.getData()).getRoot(CapConfiguration.factory);
                if(cr.hasProps()) {
                    props = deserializeProps(cr.getProps().toArray());
                }
                rmb.prepareForUpdate(cr);
            }
            
            val = f.apply(props);
            if(write) {
                CapConfiguration.Builder cb = (CapConfiguration.Builder) rmb.getSb();
                cb.setProps(serializeProps(props));
                cb.setCategory(category);
                writeMessageToEntry(rmb, data);
                db.put(txn, key, data);
            }
            txn.commit();
        }
        catch(Exception e) {
            if(txn != null) {
                txn.abort();
            }
        }
        finally {
            if(rmb != null) {
                serverObjectPool.returnObject(CapConfiguration.class, rmb);
            }
        }
        return val;
    }
}
