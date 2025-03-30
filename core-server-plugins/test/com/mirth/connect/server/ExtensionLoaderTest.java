/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.server;

import static org.junit.Assert.assertEquals;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import com.mirth.connect.model.MetaData;
import com.mirth.connect.model.converters.ObjectXMLSerializer;

public class ExtensionLoaderTest {
    private static Map<String, String> connectCoreVersions = new HashMap<String, String>();
    private static String extensionsCoreVersionsS3File;
    private static MetaData sslExtensionMetaData;
    
    private ObjectXMLSerializer serializer = ObjectXMLSerializer.getInstance();
    private ExtensionLoader extensionLoader = ExtensionLoader.getInstance();
    
    @Before
    public void setup() {
        connectCoreVersions.put("mirth-core-client", "4.6.0");
        connectCoreVersions.put("mirth-core-client-api", "4.6.1");
        connectCoreVersions.put("mirth-core-client-base", "4.6.0");
        connectCoreVersions.put("mirth-core-client-plugins", "4.6.0");
        connectCoreVersions.put("mirth-core-models", "4.6.0");
        connectCoreVersions.put("mirth-core-server-plugins", "4.6.0");
        connectCoreVersions.put("mirth-core-ui", "4.6.0");
        connectCoreVersions.put("mirth-core-util", "4.6.0");
        ExtensionLoader.setConnectCoreVersions(connectCoreVersions);
        
        try {
            URL extensionsCoreVersionsJSONFilePath = ExtensionLoaderTest.class.getResource("extensionsCoreVersions.json");
            extensionsCoreVersionsS3File = IOUtils.toString(extensionsCoreVersionsJSONFilePath, "UTF-8");
            ExtensionLoader.setExtensionsCoreVersionsS3File(extensionsCoreVersionsS3File);
            
            URL sslExtensionMetaDataFilePath = ExtensionLoaderTest.class.getResource("plugin.xml");
            sslExtensionMetaData = (MetaData) serializer.deserialize(IOUtils.toString(sslExtensionMetaDataFilePath, "UTF-8"), MetaData.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Test
    // extensionMinCoreVersions <= connectCoreVersions <= extensionMaxCoreVersions
    public void testIsExtensionCompatibleSSLMinCoreMax() {
        assertEquals(true, extensionLoader.isExtensionCompatible(sslExtensionMetaData));
    }
    
    @Test
    // connectCoreVersions < extensionMinCoreVersions, major
    public void testIsExtensionCompatibleSSLCoreLessThanMinMajor() {
        connectCoreVersions.put("mirth-core-client", "3.6.0");
        ExtensionLoader.setConnectCoreVersions(connectCoreVersions);
        assertEquals(false, extensionLoader.isExtensionCompatible(sslExtensionMetaData));
    }
    
    @Test
    // connectCoreVersions < extensionMinCoreVersions, minor
    public void testIsExtensionCompatibleSSLCoreLessThanMinMinor() {
        connectCoreVersions.put("mirth-core-client", "4.5.0");
        ExtensionLoader.setConnectCoreVersions(connectCoreVersions);
        assertEquals(false, extensionLoader.isExtensionCompatible(sslExtensionMetaData));
    }
    
    @Test
    // connectCoreVersions < extensionMinCoreVersions, patch
    // In plugin.xml, for minCoreVersions, mirth-core-client-api is set to 4.6.1
    public void testIsExtensionCompatibleSSLCoreLessThanMinPatch() {
        connectCoreVersions.put("mirth-core-client-api", "4.6.0");
        ExtensionLoader.setConnectCoreVersions(connectCoreVersions);
        assertEquals(false, extensionLoader.isExtensionCompatible(sslExtensionMetaData));
    }
    
    @Test
    // connectCoreVersions > extensionMaxCoreVersions, major
    public void testIsExtensionCompatibleSSLCoreGreaterThanMaxMajor() {
        connectCoreVersions.put("mirth-core-client", "5.7.0");
        ExtensionLoader.setConnectCoreVersions(connectCoreVersions);
        assertEquals(false, extensionLoader.isExtensionCompatible(sslExtensionMetaData));
    }
    
    @Test
    // connectCoreVersions > extensionMaxCoreVersions, minor
    public void testIsExtensionCompatibleSSLCoreGreaterThanMaxMinor() {
        connectCoreVersions.put("mirth-core-client", "4.8.0");
        ExtensionLoader.setConnectCoreVersions(connectCoreVersions);
        assertEquals(false, extensionLoader.isExtensionCompatible(sslExtensionMetaData));
    }
    
    @Test
    // connectCoreVersions > extensionMaxCoreVersions, patch
    public void testIsExtensionCompatibleSSLCoreGreaterThanMaxPatch() {
        connectCoreVersions.put("mirth-core-client", "4.7.1");
        ExtensionLoader.setConnectCoreVersions(connectCoreVersions);
        assertEquals(false, extensionLoader.isExtensionCompatible(sslExtensionMetaData));
    }
}
