package com.mirth.connect.donkey.server.data.je.comparators;

import java.io.Serializable;
import java.util.Comparator;

import com.mirth.connect.donkey.util.SerializerUtil;

public class LongPlusIntComparator implements Comparator<byte[]>, Serializable {

    @Override
    public int compare(byte[] o1, byte[] o2) {
        long l1 = SerializerUtil.bytesToLong(o1);
        long l2 = SerializerUtil.bytesToLong(o2);
        int c = 0;
        if(l1 < l2) {
            c = -1;
        }
        else if(l1 > l2) {
            c = 1;
        }
        
        if(c == 0 && o1.length == 12) {
            int i1 = SerializerUtil.bytesToInt(o1, 8);
            int i2 = SerializerUtil.bytesToInt(o2, 8);
            if(i1 < i2) {
                c = -1;
            }
            else if(i1 > i2) {
                c = 1;
            }
        }

        return c;
    }
}
