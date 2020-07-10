package com.mirth.connect.server.controllers.je;

import static com.mirth.connect.donkey.util.SerializerUtil.intToBytes;
import static com.mirth.connect.donkey.util.SerializerUtil.readMessage;
import static com.mirth.connect.donkey.util.SerializerUtil.writeMessageToEntry;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.log4j.Logger;

import com.mirth.connect.client.core.ControllerException;
import com.mirth.connect.donkey.model.message.CapnpModel.CapEvent;
import com.mirth.connect.donkey.server.BdbJeDataSource;
import com.mirth.connect.donkey.server.Donkey;
import com.mirth.connect.donkey.server.data.jdbc.ReusableMessageBuilder;
import com.mirth.connect.model.ServerEvent;
import com.mirth.connect.model.ServerEvent.Level;
import com.mirth.connect.model.ServerEvent.Outcome;
import com.mirth.connect.model.converters.ObjectXMLSerializer;
import com.mirth.connect.model.filters.EventFilter;
import com.mirth.connect.server.ExtensionLoader;
import com.mirth.connect.server.controllers.DefaultEventController;
import com.mirth.connect.server.controllers.EventController;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.SequenceStats;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;

public class BdbJeEventController extends DefaultEventController {
    private Environment env;
    private Database db;
    private Sequence seq;
    private GenericKeyedObjectPool<Class, ReusableMessageBuilder> serverObjectPool;
    private ObjectXMLSerializer objectSerializer;
    
    private StatsConfig sc = new StatsConfig();
    private String serverId;

    enum EvalResult {
        SELECTED, DROPPED, NO_MORE;
    }

    private static final Field[] EVENT_FILTER_FIELDS;
    
    private Logger logger = Logger.getLogger(BdbJeEventController.class);

    static {
        EVENT_FILTER_FIELDS = EventFilter.class.getDeclaredFields();
        for(Field f : EVENT_FILTER_FIELDS) {
            f.setAccessible(true);
        }
    }

    protected BdbJeEventController() {
        super();
    }

    public static EventController create() {
        synchronized (BdbJeEventController.class) {
            if (instance == null) {
                instance = ExtensionLoader.getInstance().getControllerInstance(EventController.class);

                if (instance == null) {
                    BdbJeEventController i = new BdbJeEventController();
                    BdbJeDataSource ds = BdbJeDataSource.getInstance();
                    i.env = ds.getBdbJeEnv();
                    String name = "event";
                    i.db = ds.getDbMap().get(name);
                    i.seq = ds.getServerSeqMap().get(name);
                    i.serverObjectPool = ds.getServerObjectPool();
                    i.objectSerializer = ObjectXMLSerializer.getInstance();
                    i.serverId = Donkey.getInstance().getConfiguration().getServerId();
                    i.sc.setFast(true);

                    instance = i;
                }
            }

            return instance;
        }
    }

    @Override
    public void insertEvent(ServerEvent serverEvent) {
        logger.debug("adding event: " + serverEvent);

        Transaction txn = null;
        ReusableMessageBuilder rmb = null;
        try {
            rmb = serverObjectPool.borrowObject(CapEvent.class);
            txn = env.beginTransaction(null, null);
            CapEvent.Builder ceb = (CapEvent.Builder)rmb.getSb();
            Map<String, String> attr = serverEvent.getAttributes();
            if(attr != null) {
                ceb.setAttributes(objectSerializer.serialize(attr));
            }
            ceb.setCreated(serverEvent.getEventTime().getTimeInMillis());
            int eventId = (int)seq.get(txn, 1);
            ceb.setId(eventId);
            ceb.setIpAddress(serverEvent.getIpAddress());
            ceb.setLevel(serverEvent.getLevel().getVal());
            ceb.setName(serverEvent.getName());
            ceb.setOutcome(serverEvent.getOutcome().getVal());
            ceb.setUserId(serverEvent.getUserId());
            
            DatabaseEntry key = new DatabaseEntry(intToBytes(eventId));
            DatabaseEntry data = new DatabaseEntry();
            writeMessageToEntry(rmb, data);
            db.put(txn, key, data);
            txn.commit();
        } catch (Exception e) {
            txn.abort();
            logger.error("Error adding event.", e);
        }
        finally {
            if(rmb != null) {
                serverObjectPool.returnObject(CapEvent.class, rmb);
            }
        }
    }

    @Override
    public Integer getMaxEventId() throws ControllerException {
        try {
            SequenceStats ss = seq.getStats(sc);
            long max = ss.getValue() - 1;
            return (int)max;
        } catch (Exception e) {
            throw new ControllerException(e);
        }
    }

    @Override
    public List<ServerEvent> getEvents(EventFilter filter, Integer offset, Integer limit) throws ControllerException {
        Transaction txn = null;
        Cursor cursor = null;
        List<ServerEvent> seList = new LinkedList<>();
        try {
            List<Field> filterFields = getNonNullFields(filter);
            txn = env.beginTransaction(null, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            OperationStatus os = null;
            if(filter.getId() != null) {
                key.setData(intToBytes(filter.getId()));
                os = db.get(txn, key, data, LockMode.READ_COMMITTED);
                if(os == OperationStatus.SUCCESS) {
                    CapEvent.Reader cr = readMessage(data.getData()).getRoot(CapEvent.factory);
                    EvalResult er = evalEvent(filter, filterFields, cr);
                    if(er == EvalResult.SELECTED) {
                        ServerEvent e = toServerEvent(cr);
                        seList.add(0, e);
                    }
                }
                return seList;
            }
            
            if(offset == null) {
                offset = 0;
            }
            
            if(offset > 0) {
                filter.setMinEventId(offset);
            }

            cursor = db.openCursor(txn, null);
            if(filter.getMinEventId() != null) {
                key.setData(intToBytes(filter.getMinEventId()));
                os = cursor.getSearchKeyRange(key, data, null);
                if(os == OperationStatus.SUCCESS) {
                    // step back
                    cursor.getPrev(key, data, null);
                }
                else { // there is nothing to scan
                    return seList;
                }
            }
            
            while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS) {
                CapEvent.Reader cr = readMessage(data.getData()).getRoot(CapEvent.factory);
                EvalResult er = evalEvent(filter, filterFields, cr);
                if(er == EvalResult.SELECTED) {
                    ServerEvent e = toServerEvent(cr);
                    seList.add(0, e);
                    limit--;
                }
                
                if(limit == 0 || er == EvalResult.NO_MORE) {
                    break;
                }
            }
        } catch (Exception e) {
            throw new ControllerException(e);
        }
        finally {
            txn.commit();
            if(cursor != null) {
                cursor.close();
            }
        }
        
        return seList;
    }

    @Override
    public Long getEventCount(EventFilter filter) throws ControllerException {
        Transaction txn = null;
        Cursor cursor = null;
        long count = 0;
        try {
            List<Field> filterFields = getNonNullFields(filter);
            txn = env.beginTransaction(null, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            cursor = db.openCursor(txn, null);

            while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS) {
                CapEvent.Reader cr = readMessage(data.getData()).getRoot(CapEvent.factory);
                EvalResult er = evalEvent(filter, filterFields, cr);
                if(er == EvalResult.SELECTED) {
                    count++;
                }
                
                if(er == EvalResult.NO_MORE) {
                    break;
                }
            }
        } catch (Exception e) {
            throw new ControllerException(e);
        }
        finally {
            txn.commit();
            if(cursor != null) {
                cursor.close();
            }
        }
        
        return count;
    }

    @Override
    public void removeAllEvents() throws ControllerException {
        logger.debug("removing all events");
        db = BdbJeDataSource.getInstance().truncateAndReopen(db);
    }
    
    private EvalResult evalEvent(EventFilter filter, List<Field> filterFields, CapEvent.Reader cr) throws Exception {
        boolean selected = false;
        EvalResult er = EvalResult.DROPPED;
        for(Field f : filterFields) {
            switch(f.getName()) {
            case "maxEventId":
                if(cr.getId() <= filter.getMaxEventId()) {
                    selected = true;
                }
                else {
                    er = EvalResult.NO_MORE;
                }
                break;

            case "minEventId":
                selected = (cr.getId() >= filter.getMinEventId());
                break;
            
            case "id":
                if(cr.getId() == filter.getId()) {
                    selected = true;
                }
                else {
                    er = EvalResult.NO_MORE;
                }
                break;
            
            case "levels":
                for(Level l : filter.getLevels()) {
                    if(l.getVal() == cr.getLevel()) {
                        selected = true;
                        break;
                    }
                }
                break;

            case "startDate":
                selected = (cr.getCreated() >= filter.getStartDate().getTimeInMillis());
                break;

            case "endDate":
                if(cr.getCreated() <= filter.getEndDate().getTimeInMillis()) {
                    selected = true;
                }
                else {
                    er = EvalResult.NO_MORE;
                }
                break;

            case "name":
                if(cr.hasName()) {
                    selected = (filter.getName().toLowerCase().equals(cr.getName().toString()));
                }
                break;
            
            case "outcome":
                selected = (cr.getOutcome() == filter.getOutcome().getVal());
                break;

            case "userId":
                selected = (cr.getUserId() == filter.getUserId());
                break;

            case "ipAddress":
                if(cr.hasIpAddress()) {
                    selected = (filter.getIpAddress().equals(cr.getIpAddress().toString()));
                }
                break;

            case "serverId":
                if(serverId.equals(filter.getServerId())) {
                    selected = true;
                }
                else {
                    er = EvalResult.NO_MORE;
                }
                break;

            default:
                    throw new IllegalArgumentException("unknown filter field " + f.getName());
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
    
    private ServerEvent toServerEvent(CapEvent.Reader cr) {
        ServerEvent se = new ServerEvent();
        if(cr.hasAttributes()) {
            Map<String, String> attrs = (Map<String, String>)objectSerializer.deserialize(cr.getAttributes().toString(), Map.class);
            se.setAttributes(attrs);
        }
        
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(cr.getCreated());
        se.setEventTime(c);
        
        se.setId(cr.getId());
        
        if(cr.hasIpAddress()) {
            se.setIpAddress(cr.getIpAddress().toString());
        }
        
        se.setLevel(Level.byVal(cr.getLevel()));
        
        if(cr.hasName()) {
            se.setName(cr.getName().toString());
        }
        
        se.setOutcome(Outcome.byVal(cr.getOutcome()));
        se.setServerId(serverId);
        se.setUserId(cr.getUserId());
        
        return se;
    }

    private List<Field> getNonNullFields(EventFilter filter) throws IllegalAccessException {
        List<Field> lst = new ArrayList<>();
        for(Field f : EVENT_FILTER_FIELDS) {
            if(f.get(filter) != null) {
                lst.add(f);
            }
        }
        return lst;
    }
}
