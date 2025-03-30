package com.mirth.connect.plugins.core.httpauth;

public enum AuthType {

    NONE("None"), BASIC("Basic Authentication"), DIGEST("Digest Authentication"), JAVASCRIPT(
            "JavaScript"), CUSTOM(
                    "Custom Java Class"), OAUTH2_VERIFICATION("OAuth 2.0 Token Verification");

    private String value;

    private AuthType(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
