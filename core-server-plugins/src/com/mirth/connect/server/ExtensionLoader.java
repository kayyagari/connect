/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.jar.JarFile;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.AndFileFilter;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.NameFileFilter;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.semver4j.Semver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.mirth.connect.donkey.util.ResourceUtil;
import com.mirth.connect.model.ConnectorMetaData;
import com.mirth.connect.model.MetaData;
import com.mirth.connect.model.PluginClass;
import com.mirth.connect.model.PluginClassCondition;
import com.mirth.connect.model.PluginMetaData;
import com.mirth.connect.model.converters.ObjectXMLSerializer;
import com.mirth.connect.server.extprops.ExtensionStatuses;
import com.mirth.connect.server.tools.ClassPathResource;
import com.mirth.connect.util.HttpUtil;
import com.mirth.connect.util.MirthSSLUtil;
import com.mirth.connect.util.PropertiesConfigurationUtil;

public class ExtensionLoader{
    @Inject
    private static ExtensionLoader instance = new ExtensionLoader();

    public static ExtensionLoader getInstance() {
        return instance;
    }
    
    private static final String EXTENSIONS_CORE_VERSIONS_S3_FILE_URL = "https://s3.amazonaws.com/downloads.mirthcorp.com/connect/extensions-core-versions/extensionsCoreVersions.json";
    private static final int TIMEOUT = 30000;
    private static final boolean HOSTNAME_VERIFICATION = true;
    private static Map<String, String> connectCoreVersions = new HashMap<String, String>();
    private static String extensionsCoreVersionsS3File;
    
    private Map<String, ConnectorMetaData> connectorMetaDataMap = new HashMap<String, ConnectorMetaData>();
    private Map<String, PluginMetaData> pluginMetaDataMap = new HashMap<String, PluginMetaData>();
    private Map<String, ConnectorMetaData> connectorProtocolsMap = new HashMap<String, ConnectorMetaData>();
    private Map<String, MetaData> invalidMetaDataMap = new HashMap<String, MetaData>();
    private Map<String, Map<String, String>> allExtensionMaxCoreVersions = new HashMap<String, Map<String, String>>();
    private boolean loadedExtensions = false;
    private ObjectXMLSerializer serializer = ObjectXMLSerializer.getInstance();
    private static Logger logger = LogManager.getLogger(ExtensionLoader.class);

    private ExtensionLoader() {}
    
    public static Map<String, String> getConnectCoreVersions() {
        return connectCoreVersions;
    }

    public static void setConnectCoreVersions(Map<String, String> connectCoreVersions) {
        ExtensionLoader.connectCoreVersions = connectCoreVersions;
    }

    public static String getExtensionsCoreVersionsS3File() {
        return extensionsCoreVersionsS3File;
    }

    public static void setExtensionsCoreVersionsS3File(String extensionsCoreVersionsS3File) {
        ExtensionLoader.extensionsCoreVersionsS3File = extensionsCoreVersionsS3File;
    }
    
    public Map<String, ConnectorMetaData> getConnectorMetaData() {
        loadExtensions();
        return connectorMetaDataMap;
    }

    public Map<String, PluginMetaData> getPluginMetaData() {
        loadExtensions();
        return pluginMetaDataMap;
    }

    public Map<String, ConnectorMetaData> getConnectorProtocols() {
        loadExtensions();
        return connectorProtocolsMap;
    }

    public Map<String, MetaData> getInvalidMetaData() {
        loadExtensions();
        return invalidMetaDataMap;
    }

    public Map<String, Map<String, String>> getExtensionMaxCoreVersions() {
        loadExtensions();
        return allExtensionMaxCoreVersions;
    }

    @SuppressWarnings("unchecked")
    public <T> Class<T> getControllerClass(Class<T> abstractClass) {
        Class<T> overrideClass = null;
        PluginClass highestPluginClassModel = null;

        ExtensionStatuses extensionStatuses = ExtensionStatuses.getInstance();

        for (PluginMetaData pluginMetaData : getPluginMetaData().values()) {
            if (extensionStatuses.isEnabled(pluginMetaData.getName())) {
                List<PluginClass> controllerClasses = pluginMetaData.getControllerClasses();

                if (controllerClasses != null) {
                    for (PluginClass controllerClassModel : controllerClasses) {
                        boolean accept = true;
                        String conditionClass = controllerClassModel.getConditionClass();
                        if (StringUtils.isNotBlank(conditionClass)) {
                            try {
                                accept = ((PluginClassCondition) Class.forName(conditionClass).newInstance()).accept(controllerClassModel);
                            } catch (Exception e) {
                                logger.warn("Error instantiating plugin condition class \"" + conditionClass + "\".");
                            }
                        }

                        if (accept) {
                            try {
                                Class<?> pluginClass = Class.forName(controllerClassModel.getName());

                                if (abstractClass.isAssignableFrom(pluginClass) && (highestPluginClassModel == null || highestPluginClassModel.getWeight() < controllerClassModel.getWeight())) {
                                    highestPluginClassModel = controllerClassModel;
                                    overrideClass = (Class<T>) pluginClass;
                                }
                            } catch (Exception e) {
                                logger.error("An error occurred while attempting to load \"" + controllerClassModel.getName() + "\" from plugin: " + pluginMetaData.getName(), e);
                            }
                        }
                    }
                }
            }
        }

        return overrideClass;
    }

    public <T> T getControllerInstance(Class<T> abstractClass) {
        Class<T> overrideClass = getControllerClass(abstractClass);

        if (overrideClass != null) {
            try {
                T instance = overrideClass.newInstance();
                logger.debug("Using custom " + abstractClass.getSimpleName() + ": " + overrideClass.getName());
                return instance;
            } catch (Exception e) {
                logger.error("An error occurred while attempting to instantiate " + abstractClass.getSimpleName() + " implementation: " + overrideClass.getName(), e);
            }
        }

        logger.debug("Using default " + abstractClass.getSimpleName());
        return null;
    }

    public boolean isExtensionCompatible(MetaData metaData) {
        if (!MapUtils.isEmpty(metaData.getMinCoreVersions())) {
            // validate extension the new way for commercial extensions
            Map<String, String> extensionMinCoreVersions = new HashMap<String, String>();
            Map<String, String> extensionMaxCoreVersions = new HashMap<String, String>();
            try {
                extensionMinCoreVersions = metaData.getMinCoreVersions();
                extensionMaxCoreVersions = getExtensionMaxCoreVersions(metaData.getPath(), metaData.getPluginVersion());
            } catch (Exception e) {
                logger.error("An error occurred while attempting to determine the extension's Core versions.", e);
                return false;
            }
            
            logger.debug("checking extension \"" + metaData.getName() + "\" version compatability: extensionMinCoreVersions=" + StringUtils.join(extensionMinCoreVersions) + " extensionMaxCoreVersions=" + StringUtils.join(extensionMaxCoreVersions) + ", connectCoreVersions=" + StringUtils.join(connectCoreVersions));
            
            // connectCoreVersions      Connect Core library versions
            // extensionMinCoreVersions minimum coreVersions for an extension
            // extensionMaxCoreVersions maximum coreVersions for an extension
            // For an extension to be valid, extensionMinCoreVersions <= connectCoreVersions <= extensionMaxCoreVersions
            // must be true for all map entries.
            for (Map.Entry<String, String> connectCoreVersionEntry : connectCoreVersions.entrySet()) {
                Semver connectCoreVersion = new Semver(connectCoreVersionEntry.getValue());
                if (extensionMinCoreVersions.containsKey(connectCoreVersionEntry.getKey())) {                    
                    if (connectCoreVersion.isLowerThan(extensionMinCoreVersions.get(connectCoreVersionEntry.getKey()))) {
                        return false;
                    }
                }
                
                if (!MapUtils.isEmpty(extensionMaxCoreVersions)) {
                    if (extensionMaxCoreVersions.containsKey(connectCoreVersionEntry.getKey())) { 
                        if (StringUtils.isNotBlank(extensionMaxCoreVersions.get(connectCoreVersionEntry.getKey())) && connectCoreVersion.isGreaterThan(extensionMaxCoreVersions.get(connectCoreVersionEntry.getKey()))) {
                            return false;
                        }
                    }
                }
            }
            
            allExtensionMaxCoreVersions.put(metaData.getPath(), extensionMaxCoreVersions);
            return true;
        } else {
            // validate extension the old way for non-commercial extensions
            String serverMirthVersion;
            try {
                serverMirthVersion = getServerVersion();
            } catch (Exception e) {
                logger.error("An error occurred while attempting to determine the current server version.", e);
                return false;
            }
            
            String[] extensionMirthVersions = metaData.getMirthVersion().split(",");

            logger.debug("checking extension \"" + metaData.getName() + "\" version compatability: versions=" + ArrayUtils.toString(extensionMirthVersions) + ", server=" + serverMirthVersion);

            // if there is no build version, just use the patch version
            if (serverMirthVersion.split("\\.").length == 4) {
                serverMirthVersion = serverMirthVersion.substring(0, serverMirthVersion.lastIndexOf('.'));
            }

            for (int i = 0; i < extensionMirthVersions.length; i++) {
                if (extensionMirthVersions[i].trim().equals(serverMirthVersion)) {
                    return true;
                }
            }

            return false;
        }
    }

    /**
     * Loads the metadata files (plugin.xml, source.xml, destination.xml) for all extensions of the
     * specified type. If this function fails to parse the metadata file for an extension, it will
     * skip it and continue.
     */
    private synchronized void loadExtensions() {
        if (!loadedExtensions) {
            try {
                ManifestDirectory coreLibServerDirMirthLibs = new ManifestDirectory("core-lib/server");
                coreLibServerDirMirthLibs.setIncludePrefix("mirth-core-");
                ManifestDirectory coreLibSharedDirMirthLibs = new ManifestDirectory("core-lib/shared");
                coreLibSharedDirMirthLibs.setIncludePrefix("mirth-core-");
                ManifestDirectory coreLibUiDirMirthLibs = new ManifestDirectory("core-lib/ui");
                coreLibUiDirMirthLibs.setIncludePrefix("mirth-core-");
                
                List<ManifestEntry> manifestListConnectCoreLibs = new ArrayList<ManifestEntry>();
                manifestListConnectCoreLibs.add(coreLibServerDirMirthLibs);
                manifestListConnectCoreLibs.add(coreLibSharedDirMirthLibs);
                manifestListConnectCoreLibs.add(coreLibUiDirMirthLibs);
                
                ManifestEntry[] manifestConnectCoreLibs = manifestListConnectCoreLibs.toArray(new ManifestEntry[manifestListConnectCoreLibs.size()]);
                
                // Get the current Connect Core library versions
                initializeCoreVersionsFields(manifestConnectCoreLibs);
                
                // match all of the file names for the extension
                IOFileFilter nameFileFilter = new NameFileFilter(new String[] { "plugin.xml",
                        "source.xml", "destination.xml" });
                // this is probably not needed, but we don't want to pick up directories,
                // so we AND the two filters
                IOFileFilter andFileFilter = new AndFileFilter(nameFileFilter, FileFilterUtils.fileFileFilter());
                // this is directory where extensions are located
                File extensionPath = new File(getExtensionsPath());
                // do a recursive scan for extension files
                Collection<File> extensionFiles = FileUtils.listFiles(extensionPath, andFileFilter, FileFilterUtils.trueFileFilter());

                for (File extensionFile : extensionFiles) {
                    try {
                        MetaData metaData = (MetaData) serializer.deserialize(FileUtils.readFileToString(extensionFile), MetaData.class);

                        if (isExtensionCompatible(metaData)) {
                            if (metaData instanceof ConnectorMetaData) {
                                ConnectorMetaData connectorMetaData = (ConnectorMetaData) metaData;
                                connectorMetaDataMap.put(connectorMetaData.getName(), connectorMetaData);

                                if (StringUtils.contains(connectorMetaData.getProtocol(), ":")) {
                                    for (String protocol : connectorMetaData.getProtocol().split(":")) {
                                        connectorProtocolsMap.put(protocol, connectorMetaData);
                                    }
                                } else {
                                    connectorProtocolsMap.put(connectorMetaData.getProtocol(), connectorMetaData);
                                }
                            } else if (metaData instanceof PluginMetaData) {
                                pluginMetaDataMap.put(metaData.getName(), (PluginMetaData) metaData);
                            }
                        } else {
                            logger.error("Extension \"" + metaData.getName() + "\" is not compatible with this version of Mirth Connect and was not loaded. Please install a compatible version.");
                            invalidMetaDataMap.put(metaData.getName(), metaData);
                        }
                    } catch (Exception e) {
                        logger.error("Error reading or parsing extension metadata file: " + extensionFile.getName(), e);
                    }
                }
            } catch (Exception e) {
                logger.error("Error loading extension metadata.", e);
            } finally {
                loadedExtensions = true;
            }
        }
    }

    /**
     * If in an IDE, extensions will be on the classpath as a resource. If that's the case, use that
     * directory. Otherwise, use the mirth home directory and append extensions.
     * 
     * @return
     */
    private String getExtensionsPath() {
        if (ClassPathResource.getResourceURI("extensions") != null) {
            return ClassPathResource.getResourceURI("extensions").getPath() + File.separator;
        } else {
            return new File(ClassPathResource.getResourceURI("mirth.properties")).getParentFile().getParent() + File.separator + "extensions" + File.separator;
        }
    }

    private String getServerVersion() throws FileNotFoundException, ConfigurationException {
        PropertiesConfiguration versionConfig = PropertiesConfigurationUtil.create();
        
        InputStream versionPropertiesStream = null;
        try {
            versionPropertiesStream = ResourceUtil.getResourceStream(ExtensionLoader.class, "version.properties");
            versionConfig = PropertiesConfigurationUtil.create(versionPropertiesStream);
        } finally {
            ResourceUtil.closeResourceQuietly(versionPropertiesStream);
        }
        
        return versionConfig.getString("mirth.version");
    }
    
    private static void initializeCoreVersionsFields(ManifestEntry[] manifestEntries) {        
        connectCoreVersions = getConnectCoreVersions(manifestEntries);
        
        // get extensions Core versions JSON from S3
        extensionsCoreVersionsS3File = getExtensionsCoreVersionsFileFromS3();
    }

    /**
     * Get the Connect Core library versions by going through each Core library's *.jar file,
     * loading its *.version.properties file, and getting its library.version property.
     * @param manifestEntries An array of directories where the Connect Core library *.jar files are located
     * @return Map<String, String> == Map<coreLibaryName, coreLibraryVersion>
     */
    private static Map<String, String> getConnectCoreVersions(ManifestEntry[] manifestEntries) {
        Map<String, String> connectCoreVersions = new HashMap<String, String>();
        
        for (ManifestEntry manifestEntry : manifestEntries) {
            File manifestEntryFile = new File(manifestEntry.getName());
            
            if (manifestEntryFile.exists()) {
                if (manifestEntryFile.isDirectory()) {
                    ManifestDirectory manifestDir = (ManifestDirectory) manifestEntry;
                    IOFileFilter fileFilter = FileFilterUtils.fileFileFilter();
                    
                    if (manifestDir.getIncludePrefix() != null) {
                        fileFilter = FileFilterUtils.and(fileFilter, new PrefixFileFilter(manifestDir.getIncludePrefix()));
                    }
                    
                    Collection<File> pathFiles = FileUtils.listFiles(manifestEntryFile, fileFilter, FileFilterUtils.trueFileFilter());
                    
                    for (File pathFile : pathFiles) {
                        JarFile connectCoreLibraryJarFile = null;
                        try {
                            // e.g. mirth-core-server-plugins-4.6.0
                            String pathFilename = FilenameUtils.getBaseName(pathFile.toString());
                            // e.g. mirth-core-server-plugins
                            String connectCoreLibraryVersionPropertiesFileName = pathFilename.substring(0, pathFilename.length() - 6);
                            String connectCoreLibraryVersionProperty = "library.version";
                            connectCoreLibraryJarFile = new JarFile(pathFile);
                            
                            Properties connectCoreLibraryVersionProperties = new Properties();
                            connectCoreLibraryVersionProperties.load(connectCoreLibraryJarFile.getInputStream(connectCoreLibraryJarFile.getJarEntry(connectCoreLibraryVersionPropertiesFileName + ".version.properties")));
                            connectCoreVersions.put(connectCoreLibraryVersionPropertiesFileName, connectCoreLibraryVersionProperties.getProperty(connectCoreLibraryVersionProperty));
                        } catch (IOException e) {
                            logger.error("An error occurred while attempting to determine the Connect Core versions.", e);
                        } finally {
                            try {
                                if (connectCoreLibraryJarFile != null) {
                                    connectCoreLibraryJarFile.close();
                                }
                            } catch (IOException e) {
                                logger.error("Error closing connectCoreLibraryJarFile.", e);
                            }
                        }
                    }
                }
            } else {
                logger.warn("manifest path not found: " + manifestEntryFile.getAbsolutePath());
            }
        }
        
        return connectCoreVersions;
    }
    
    /**
     * Get the max Core library versions for an extension. If the max doesn't exist for an extension's Core library, then set its max entry to its min entry.
     * 
     * @param pluginPath                Plugin name
     * @param pluginVersion             Plugin version
     * @param extensionMinCoreVersions  Extension min Core versions to use as max Core versions if max is empty or missing from the manifest .json file
     * @return Map<String, String> == Map<coreLibaryName, pluginMaxCoreLibraryVersion>
     * @throws JsonProcessingException
     * @throws JsonMappingException
     */
    private Map<String, String> getExtensionMaxCoreVersions(String pluginPath, String pluginVersion) throws JsonMappingException, JsonProcessingException {
        Map<String, String> extensionMaxCoreVersions = new HashMap<String, String>();

        if (!StringUtils.isEmpty(extensionsCoreVersionsS3File)) {
            ObjectMapper mapper = new ObjectMapper();
            
            JsonNode extensionsCoreVersionsS3FileTree = mapper.readTree(extensionsCoreVersionsS3File);
            
            if (extensionsCoreVersionsS3FileTree.has("extensionsData") &&
                extensionsCoreVersionsS3FileTree.get("extensionsData").has(pluginPath) &&
                extensionsCoreVersionsS3FileTree.get("extensionsData").get(pluginPath).has(pluginVersion) &&
                extensionsCoreVersionsS3FileTree.get("extensionsData").get(pluginPath).get(pluginVersion).has("coreVersions")) {
                // Map.Entry<String, JsonNode> == Map.Entry<coreLibraryName, JsonNode(min, max)>
                Iterator<Map.Entry<String, JsonNode>> extensionCoreVersionsS3FileIt = extensionsCoreVersionsS3FileTree.get("extensionsData").get(pluginPath).get(pluginVersion).get("coreVersions").fields();        
                while(extensionCoreVersionsS3FileIt.hasNext()) {
                    Map.Entry<String, JsonNode> extensionCoreVersionS3FileEntry = extensionCoreVersionsS3FileIt.next();

                    if (extensionCoreVersionS3FileEntry.getValue().has("max") && StringUtils.isNotBlank(extensionCoreVersionS3FileEntry.getValue().get("max").textValue())) {
                        extensionMaxCoreVersions.put(extensionCoreVersionS3FileEntry.getKey(), extensionCoreVersionS3FileEntry.getValue().get("max").textValue());
                    }
                }
            }
        }
        
        return extensionMaxCoreVersions;
    }
    
    /**
     * Get the JSON manifest file that contains all extensions' Core libraries min and max versions.
     * 
     * @return String JSON manifest file from S3 or "" on error
     */
    private static String getExtensionsCoreVersionsFileFromS3() {
        return HttpUtil.executeGetRequest(EXTENSIONS_CORE_VERSIONS_S3_FILE_URL, TIMEOUT, HOSTNAME_VERIFICATION, MirthSSLUtil.DEFAULT_HTTPS_CLIENT_PROTOCOLS, MirthSSLUtil.DEFAULT_HTTPS_CIPHER_SUITES);
    }
}
