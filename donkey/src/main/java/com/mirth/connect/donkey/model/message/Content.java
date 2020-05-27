/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.donkey.model.message;

import java.io.Serializable;

public abstract class Content implements Serializable {
    private boolean encrypted = false;

    public Content() {}

    public boolean isEncrypted() {
        return encrypted;
    }

    public void setEncrypted(boolean encrypted) {
        this.encrypted = encrypted;
    }

    public abstract Object getContent();

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (encrypted ? 1231 : 1237);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
//        if (getClass() != obj.getClass()) {
//            return false;
//        }
        Content other = (Content) obj;
        if (encrypted != other.encrypted) {
            return false;
        }
        return true;
    }
}
