/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.donkey.model.message.attachment;

import java.io.Serializable;
import java.util.Arrays;

import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("attachment")
public class Attachment implements Serializable {
    protected String id;
    protected byte[] content;
    protected String type;
    protected boolean encrypt;

    public Attachment() {

    }

    public Attachment(String id, byte[] content, String type) {
        this.id = id;
        this.content = content;
        this.setType(type);
    }

    public String getAttachmentId() {
        return "${ATTACH:" + id + "}";
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isEncrypted() {
        return encrypt;
    }

    public void setEncrypted(boolean encrypt) {
        this.encrypt = encrypt;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(content);
        result = prime * result + (encrypt ? 1231 : 1237);
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
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
        Attachment other = (Attachment) obj;
        if (!Arrays.equals(content, other.content)) {
            return false;
        }
        if (encrypt != other.encrypt) {
            return false;
        }
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!id.equals(other.id)) {
            return false;
        }
        if (type == null) {
            if (other.type != null) {
                return false;
            }
        } else if (!type.equals(other.type)) {
            return false;
        }
        return true;
    }
}
