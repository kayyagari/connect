/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.server.util.javascript;

import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.Script;
import org.mozilla.javascript.Scriptable;

import com.mirth.connect.donkey.server.channel.IConnector;
import com.mirth.connect.donkey.server.channel.IDestinationConnector;
import com.mirth.connect.donkey.server.channel.ISourceConnector;
import com.mirth.connect.donkey.util.ThreadUtils;

public abstract class JavaScriptTask<T> implements Callable<T> {

    private Logger logger = LogManager.getLogger(JavaScriptTask.class);
    private ContextFactory contextFactory;
    private String threadName;
    private Context context;
    private boolean contextCreated = false;

    public JavaScriptTask(ContextFactory contextFactory, String name) {
        this(contextFactory, name, null, null);
    }

    public JavaScriptTask(ContextFactory contextFactory, String name, String channelId, String channelName) {
        this(contextFactory, name, channelId, channelName, null, null);
    }

    public JavaScriptTask(ContextFactory contextFactory, ISourceConnector sourceConnector) {
        this(contextFactory, sourceConnector.getConnectorProperties().getName(), sourceConnector);
    }

    public JavaScriptTask(ContextFactory contextFactory, String name, ISourceConnector sourceConnector) {
        this(contextFactory, name, sourceConnector.getChannelId(), sourceConnector.getChannelName(), sourceConnector.getMetaDataId(), null);
    }

    public JavaScriptTask(ContextFactory contextFactory, IDestinationConnector destinationConnector) {
        this(contextFactory, destinationConnector.getConnectorProperties().getName(), destinationConnector.getChannelId(), destinationConnector.getChannelName(), destinationConnector.getMetaDataId(), destinationConnector.getConnectorName());
    }

    public JavaScriptTask(ContextFactory contextFactory, IConnector connector) {
        this(contextFactory, connector.getConnectorProperties().getName(), connector);
    }

    public JavaScriptTask(ContextFactory contextFactory, String name, IConnector connector) {
        this(contextFactory);
        if (connector instanceof ISourceConnector) {
            init(name, connector.getChannelId(), connector.getChannelName(), connector.getMetaDataId(), null);
        } else {
            init(name, connector.getChannelId(), connector.getChannelName(), connector.getMetaDataId(), connector.getConnectorName());
        }
    }

    private JavaScriptTask(ContextFactory contextFactory, String name, String channelId, String channelName, Integer metaDataId, String destinationName) {
        this(contextFactory);
        init(name, channelId, channelName, metaDataId, destinationName);
    }

    private JavaScriptTask(ContextFactory contextFactory) {
        this.contextFactory = contextFactory;
    }

    private void init(String name, String channelId, String channelName, Integer metaDataId, String destinationName) {
        StringBuilder builder = new StringBuilder(name).append(" JavaScript Task");
        if (StringUtils.isNotEmpty(channelName)) {
            builder.append(" on ").append(channelName);
            if (StringUtils.isNotEmpty(channelId)) {
                builder.append(" (").append(channelId).append(')');
            }

            if (metaDataId != null && metaDataId > 0) {
                builder.append(',');
                if (StringUtils.isNotEmpty(destinationName)) {
                    builder.append(' ').append(destinationName);
                }
                builder.append(" (").append(metaDataId).append(')');
            }
        }
        threadName = builder.toString();
    }

    public ContextFactory getContextFactory() {
        return contextFactory;
    }

    public void setContextFactory(ContextFactory contextFactory) {
        this.contextFactory = contextFactory;
    }

    public Context getContext() {
        return context;
    }

    public abstract T doCall() throws Exception;

    @Override
    public final T call() throws Exception {
        String originalThreadName = Thread.currentThread().getName();
        try {
            Thread.currentThread().setName(threadName + " < " + originalThreadName);
            return doCall();
        } finally {
            Thread.currentThread().setName(originalThreadName);
        }
    }

    public Object executeScript(Script compiledScript, Scriptable scope) throws InterruptedException {
        Thread currentThread = Thread.currentThread();

        try {
            // if the executor is halting this task, we don't want to initialize the context yet
            synchronized (this) {
                ThreadUtils.checkInterruptedStatus();
                context = Context.getCurrentContext();
                currentThread.setContextClassLoader(((ContextFactory) contextFactory).getApplicationClassLoader());
                logger.debug(StringUtils.defaultString(StringUtils.trimToNull(getClass().getSimpleName()), getClass().getName()) + " using context factory: " + contextFactory.hashCode());

                /*
                 * This should never be called but exists in case executeScript is called from a
                 * different thread than the one that entered the context.
                 */
                if (context == null) {
                    contextCreated = true;
                    context = JavaScriptCoreUtil.getContext(contextFactory);
                }

                if (context instanceof MirthContext) {
                    ((MirthContext) context).setRunning(true);
                }
            }

            if (currentThread instanceof MirthJavaScriptThread) {
                MirthJavaScriptThread mirthThread = (MirthJavaScriptThread) currentThread;
                mirthThread.setContext(context);
                mirthThread.setScope(scope);
            }

            return compiledScript.exec(context, scope);
        } finally {
            if (contextCreated) {
                Context.exit();
                contextCreated = false;
            }

            if (currentThread instanceof MirthJavaScriptThread) {
                MirthJavaScriptThread mirthThread = (MirthJavaScriptThread) currentThread;
                mirthThread.setContext(null);
                mirthThread.setScope(null);
            }
        }
    }
}
