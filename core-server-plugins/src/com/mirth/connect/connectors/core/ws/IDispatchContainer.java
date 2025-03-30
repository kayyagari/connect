package com.mirth.connect.connectors.core.ws;

import java.io.File;
import java.util.List;
import java.util.Map;

public interface IDispatchContainer {

	String getCurrentWsdlUrl();

    void setCurrentWsdlUrl(String currentWsdlUrl);

    String getCurrentUsername();

    void setCurrentUsername(String currentUsername);

    String getCurrentPassword();

    void setCurrentPassword(String currentPassword);

    String getCurrentServiceName();

    void setCurrentServiceName(String currentServiceName);

    String getCurrentPortName();

    void setCurrentPortName(String currentPortName);

    List<File> getTempFiles();

    Map<String, List<String>> getDefaultRequestHeaders();

    void setDefaultRequestHeaders(Map<String, List<String>> defaultRequestHeaders);
    
}
