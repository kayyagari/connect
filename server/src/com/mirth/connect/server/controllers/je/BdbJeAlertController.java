package com.mirth.connect.server.controllers.je;

import static com.mirth.connect.donkey.util.SerializerUtil.readMessage;
import static com.mirth.connect.donkey.util.SerializerUtil.writeMessageToEntry;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.log4j.Logger;

import com.mirth.connect.client.core.ControllerException;
import com.mirth.connect.donkey.model.message.CapnpModel.CapAlert;
import com.mirth.connect.donkey.server.BdbJeDataSource;
import com.mirth.connect.donkey.server.data.jdbc.ReusableMessageBuilder;
import com.mirth.connect.model.alert.AlertModel;
import com.mirth.connect.model.converters.ObjectXMLSerializer;
import com.mirth.connect.server.ExtensionLoader;
import com.mirth.connect.server.controllers.AlertController;
import com.mirth.connect.server.controllers.DefaultAlertController;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class BdbJeAlertController extends DefaultAlertController {

    private Logger logger = Logger.getLogger(BdbJeAlertController.class);

    private Environment env;
    private Database db;
    private GenericKeyedObjectPool<Class, ReusableMessageBuilder> serverObjectPool;

    protected BdbJeAlertController() {
    }

    public static AlertController create() {
        synchronized (BdbJeAlertController.class) {
            if (instance == null) {
                instance = ExtensionLoader.getInstance().getControllerInstance(AlertController.class);

                if (instance == null) {
                    BdbJeDataSource ds = BdbJeDataSource.getInstance();
                    BdbJeAlertController i = new BdbJeAlertController();
                    i.env = ds.getBdbJeEnv();
                    String name = "alert";
                    i.db = ds.getDbMap().get(name);
                    i.serverObjectPool = ds.getServerObjectPool();
                    instance = i;
                }
            }

            return instance;
        }
    }

    @Override
    public AlertModel getAlert(String alertId) throws ControllerException {
        logger.debug("getting alert");

        Transaction txn = null;
        try {
            AlertModel am = null;
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            txn = env.beginTransaction(null, null);
            key.setData(alertId.getBytes(utf8));
            OperationStatus os = db.get(txn, key, data, LockMode.READ_COMMITTED);
            if(os == OperationStatus.SUCCESS) {
                ObjectXMLSerializer serializer = ObjectXMLSerializer.getInstance();
                try {
                    CapAlert.Reader ar = readMessage(data.getData()).getRoot(CapAlert.factory);
                    am = serializer.deserialize(ar.getAlert().toString(), AlertModel.class);
                } catch (Exception e) {
                    logger.error("Failed to load alert " + alertId, e);
                }
            }
            txn.commit();
            return am;
        } catch (Exception e) {
            if(txn != null) {
                txn.abort();
            }
            throw new ControllerException(e);
        }
    }

    @Override
    public List<AlertModel> getAlerts() throws ControllerException {
        logger.debug("getting alerts");

        Transaction txn = null;
        try {
            ObjectXMLSerializer serializer = ObjectXMLSerializer.getInstance();
            List<AlertModel> alerts = new ArrayList<AlertModel>();

            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            txn = env.beginTransaction(null, null);
            Cursor cursor = db.openCursor(txn, null);
            while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS) {
                CapAlert.Reader ar = readMessage(data.getData()).getRoot(CapAlert.factory);
                try {
                    alerts.add(serializer.deserialize(ar.getAlert().toString(), AlertModel.class));
                } catch (Exception e) {
                    logger.warn("Failed to load alert " + ar.getId().toString(), e);
                }
            }
            cursor.close();

            return alerts;
        } catch (Exception e) {
            if(txn != null) {
                txn.abort();
            }
            throw new ControllerException(e);
        }
    }

    @Override
    public void updateAlert(AlertModel alert) throws ControllerException {
        if (alert == null) {
            return;
        }

        Transaction txn = null;
        ReusableMessageBuilder rmb = null;
        try {
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            txn = env.beginTransaction(null, null);
            if (alert.getName() != null) {
                String alertName = alert.getName().toLowerCase();
                String id = alert.getId();
                Cursor cursor = db.openCursor(txn, null);
                boolean duplicateName = false;
                while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS) {
                    CapAlert.Reader ar = readMessage(data.getData()).getRoot(CapAlert.factory);
                    if(id.equals(ar.getId().toString())) {
                        continue;
                    }
                    if(ar.hasName()) {
                        if(alertName.equals(ar.getName().toString().toLowerCase())) {
                            duplicateName = true;
                            break;
                        }
                    }
                }
                cursor.close();
                if (duplicateName) {
                    throw new ControllerException("An alert with that name already exists.");
                }
            }

            key.setData(alert.getId().getBytes(utf8));
            boolean alertExists = db.get(txn, key, data, LockMode.READ_COMMITTED) == OperationStatus.SUCCESS;

            if (alertExists) {
                disableAlert(alert.getId());

                logger.debug("updating alert");
            } else {
                logger.debug("adding alert");
            }

            ObjectXMLSerializer serializer = ObjectXMLSerializer.getInstance();
            rmb = serverObjectPool.borrowObject(CapAlert.class);
            CapAlert.Builder ab = (CapAlert.Builder) rmb.getSb();
            ab.setId(alert.getId());
            ab.setName(alert.getName());
            ab.setAlert(serializer.serialize(alert));
            writeMessageToEntry(rmb, data);
            db.put(txn, key, data);
            txn.commit();
            if (alert.isEnabled()) {
                enableAlert(alert);
            }
        } catch (Exception e) {
            if(txn != null) {
                txn.abort();
            }
            throw new ControllerException(e);
        }
        finally {
            if(rmb != null) {
                serverObjectPool.returnObject(CapAlert.class, rmb);
            }
        }
    }

    @Override
    public void removeAlert(String alertId) throws ControllerException {
        logger.debug("removing alert");

        Transaction txn = null;
        try {

            if (alertId != null) {
                disableAlert(alertId);
                DatabaseEntry key = new DatabaseEntry();
                txn = env.beginTransaction(null, null);
                key.setData(alertId.getBytes(utf8));
                db.delete(txn, key);
                txn.commit();
            }
        } catch (Exception e) {
            if(txn != null) {
                txn.abort();
            }
            throw new ControllerException(e);
        }
    }

    public void vacuumAlertTable() {
        // not applicable
    }
}
