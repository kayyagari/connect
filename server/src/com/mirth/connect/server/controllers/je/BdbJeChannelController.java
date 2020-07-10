package com.mirth.connect.server.controllers.je;

import static com.mirth.connect.donkey.util.SerializerUtil.writeMessageToEntry;

import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.log4j.Logger;

import com.mirth.connect.client.core.ControllerException;
import com.mirth.connect.donkey.model.message.CapnpModel.CapChannel;
import com.mirth.connect.donkey.model.message.CapnpModel.CapChannelGroup;
import com.mirth.connect.donkey.server.BdbJeDataSource;
import com.mirth.connect.donkey.server.data.jdbc.ReusableMessageBuilder;
import com.mirth.connect.model.Channel;
import com.mirth.connect.model.ChannelDependency;
import com.mirth.connect.model.ChannelGroup;
import com.mirth.connect.model.ChannelMetadata;
import com.mirth.connect.model.ChannelTag;
import com.mirth.connect.model.ServerEventContext;
import com.mirth.connect.model.converters.ObjectXMLSerializer;
import com.mirth.connect.plugins.ChannelPlugin;
import com.mirth.connect.server.ExtensionLoader;
import com.mirth.connect.server.controllers.ChannelController;
import com.mirth.connect.server.controllers.ConfigurationController;
import com.mirth.connect.server.controllers.ControllerFactory;
import com.mirth.connect.server.controllers.DefaultChannelController;
import com.mirth.connect.server.util.StatementLock;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;

public class BdbJeChannelController extends DefaultChannelController {
    private Environment env;
    private Database channelDb;
    private Database channelGroupDb;
    private GenericKeyedObjectPool<Class, ReusableMessageBuilder> serverObjectPool;
    private ObjectXMLSerializer objectSerializer;

    private static Logger logger = Logger.getLogger(BdbJeChannelController.class);
    
    protected BdbJeChannelController() {

    }

    public static ChannelController create() {
        synchronized (BdbJeChannelController.class) {
            if (instance == null) {
                instance = ExtensionLoader.getInstance()
                        .getControllerInstance(ChannelController.class);

                if (instance == null) {
                    BdbJeDataSource ds = BdbJeDataSource.getInstance();
                    BdbJeChannelController i = new BdbJeChannelController();
                    i.env = ds.getBdbJeEnv();
                    String name = "channel";
                    i.channelDb = ds.getDbMap().get(name);
                    i.channelGroupDb = ds.getDbMap().get("channel_group");
                    i.serverObjectPool = ds.getServerObjectPool();
                    i.objectSerializer = ObjectXMLSerializer.getInstance();
                    i.channelCache = new BdbJeChannelCache(i.env, i.channelDb);
                    i.channelGroupCache = new BdbJeChannelGroupCache(i.env, i.channelGroupDb);
                    instance = i;
                }
            }

            return instance;
        }
    }

    @Override
    protected void updateChannel_db(Channel channel) throws Exception {
        upsertChannel_db(channel);
    }

    @Override
    protected void addChannel_db(Channel channel) throws Exception {
        upsertChannel_db(channel);
    }

    private void upsertChannel_db(Channel channel) throws Exception {
        Transaction txn = null;
        ReusableMessageBuilder rmb = null;
        try {
            txn = env.beginTransaction(null, null);

            rmb = serverObjectPool.borrowObject(CapChannel.class);
            CapChannel.Builder ccb = (CapChannel.Builder)rmb.getSb();
            
            String cxml = objectSerializer.serialize(channel);
            ccb.setChannel(cxml);
            ccb.setId(channel.getId());
            ccb.setName(channel.getName());
            ccb.setRevision(channel.getRevision());
            
            DatabaseEntry key = new DatabaseEntry(channel.getId().getBytes(utf8));
            DatabaseEntry data = new DatabaseEntry();
            writeMessageToEntry(rmb, data);
            channelDb.put(txn, key, data);
            txn.commit();
        }
        catch(Exception e) {
            txn.abort();
            throw e;
        }
        finally {
            if(rmb != null) {
                serverObjectPool.returnObject(CapChannel.class, rmb);
            }
        }
    }

    @Override
    public synchronized void removeChannel(Channel channel, ServerEventContext context) throws ControllerException {
        /*
         * Methods that update the channel must be synchronized to ensure the channel cache and
         * database never contain different versions of a channel.
         */

        logger.debug("removing channel");

        if (channel != null && ControllerFactory.getFactory().createEngineController().isDeployed(channel.getId())) {
            logger.warn("Cannot remove deployed channel.");
            return;
        }

        StatementLock.getInstance(VACUUM_LOCK_CHANNEL_STATEMENT_ID).writeLock();
        Transaction txn = null;
        try {
            //TODO combine and organize these.
            // Delete the "d_" tables and the channel record from "d_channels"
            com.mirth.connect.donkey.server.controllers.ChannelController.getInstance().removeChannel(channel.getId());
            // Delete the channel record from the "channel" table
            txn = env.beginTransaction(null, null);
            DatabaseEntry key = new DatabaseEntry(channel.getId().getBytes(utf8));
            channelDb.delete(txn, key);

            // Update any groups that contained this channel
            Set<ChannelGroup> groups = new HashSet<ChannelGroup>(channelGroupCache.getAllItems().values());
            boolean groupsChanged = false;
            for (ChannelGroup group : groups) {
                for (Iterator<Channel> it = group.getChannels().iterator(); it.hasNext();) {
                    if (channel.getId().equals(it.next().getId())) {
                        it.remove();
                        groupsChanged = true;
                    }
                }
            }
            if (groupsChanged) {
                updateChannelGroups(txn, groups, new HashSet<String>(), true);
            }

            // Remove any dependencies that were tied to this channel
            ConfigurationController configurationController = ControllerFactory.getFactory().createConfigurationController();
            Set<ChannelDependency> dependencies = configurationController.getChannelDependencies();
            boolean dependenciesChanged = false;
            for (Iterator<ChannelDependency> it = dependencies.iterator(); it.hasNext();) {
                ChannelDependency dependency = it.next();
                if (channel.getId().equals(dependency.getDependentId()) || channel.getId().equals(dependency.getDependencyId())) {
                    it.remove();
                    dependenciesChanged = true;
                }
            }
            if (dependenciesChanged) {
                configurationController.setChannelDependencies(dependencies);
            }

            // Update the metadata
            Map<String, ChannelMetadata> metadataMap = configurationController.getChannelMetadata();
            if (metadataMap.remove(channel.getId()) != null) {
                configurationController.setChannelMetadata(metadataMap);
            }

            // Remove any tags this channel was a part of
            boolean tagsRemoved = false;
            Set<ChannelTag> tags = configurationController.getChannelTags();
            for (ChannelTag tag : tags) {
                if (tag.getChannelIds().contains(channel.getId())) {
                    tagsRemoved = true;
                    tag.getChannelIds().remove(channel.getId());
                }
            }

            if (tagsRemoved) {
                configurationController.setChannelTags(tags);
            }

            // invoke the channel plugins
            for (ChannelPlugin channelPlugin : extensionController.getChannelPlugins().values()) {
                channelPlugin.remove(channel, context);
            }
            
            txn.commit();
        } catch (Exception e) {
            txn.abort();
            throw new ControllerException(e);
        } finally {
            StatementLock.getInstance(VACUUM_LOCK_CHANNEL_STATEMENT_ID).writeUnlock();
        }
    }

    @Override
    public synchronized boolean updateChannelGroups(Set<ChannelGroup> channelGroups, Set<String> removedChannelGroupIds, boolean override) throws ControllerException {
        Transaction txn = null;
        boolean result = false;
        try {
            txn = env.beginTransaction(null, null);
            result = updateChannelGroups(txn, channelGroups, removedChannelGroupIds, override);
            txn.commit();
        }
        catch(ControllerException e) {
            txn.abort();
            throw e;
        }
        
        return result;
    }

    @Override
    protected void updateChannelGroups_db(Transaction txn, Set<ChannelGroup> channelGroups, List<ChannelGroup> groupsToRemove, Set<String> unchangedGroupIds) throws ControllerException {
        // Remove groups
        DatabaseEntry key = new DatabaseEntry();
        StatementLock.getInstance(VACUUM_LOCK_CHANNEL_GROUP_STATEMENT_ID).readLock();
        try {
            for (ChannelGroup group : groupsToRemove) {
                key.setData(group.getId().getBytes(utf8));
                channelGroupDb.delete(txn, key);
            }
        } catch (Exception e) {
            throw new ControllerException(e);
        } finally {
            StatementLock.getInstance(VACUUM_LOCK_CHANNEL_GROUP_STATEMENT_ID).readUnlock();
        }

        // Insert or update groups
        StatementLock.getInstance(VACUUM_LOCK_CHANNEL_GROUP_STATEMENT_ID).readLock();
        try {
            DatabaseEntry data = new DatabaseEntry();
            for (ChannelGroup group : channelGroups) {
                if (!unchangedGroupIds.contains(group.getId())) {
                    group.setLastModified(Calendar.getInstance());
                    ReusableMessageBuilder rmb = serverObjectPool.borrowObject(CapChannelGroup.class);
                    CapChannelGroup.Builder ccg = (CapChannelGroup.Builder) rmb.getSb();
                    ccg.setChannelGroup(objectSerializer.serialize(group));
                    ccg.setId(group.getId());
                    ccg.setName(group.getName());
                    ccg.setRevision(group.getRevision());

                    key.setData(group.getId().getBytes(utf8));
                    
                    writeMessageToEntry(rmb, data);
                    // If its a new group, insert it, otherwise, update it
                    if (channelGroupCache.getCachedItemById(group.getId()) == null) {
                        logger.debug("Inserting channel group");
                        channelGroupDb.put(txn, key, data);
                    } else {
                        logger.debug("Updating channel group");
                        channelGroupDb.put(txn, key, data);
                    }
                    
                    serverObjectPool.returnObject(CapChannelGroup.class, rmb);
                }
            }
        } catch (Exception e) {
            throw new ControllerException(e);
        } finally {
            StatementLock.getInstance(VACUUM_LOCK_CHANNEL_GROUP_STATEMENT_ID).readUnlock();
        }
    }

    @Override
    public void vacuumChannelTable() {
    }
    
    @Override
    public void vacuumChannelGroupTable() {
    }
}
