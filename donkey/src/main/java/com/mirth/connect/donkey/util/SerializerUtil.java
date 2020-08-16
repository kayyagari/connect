package com.mirth.connect.donkey.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.capnproto.ArrayInputStream;
import org.capnproto.ArrayOutputStream;
import org.capnproto.DecodeException;
import org.capnproto.MessageBuilder;
import org.capnproto.MessageReader;
import org.capnproto.ReaderOptions;
import org.capnproto.SerializePacked;

import com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent;
import com.mirth.connect.donkey.model.message.ContentType;
import com.mirth.connect.donkey.server.data.jdbc.ReusableMessageBuilder;
import com.sleepycat.je.DatabaseEntry;

import io.netty.buffer.ByteBuf;

public class SerializerUtil {
    public static final int BITS_PER_BYTE = 8;
    public static final int BITS_PER_POINTER = 64;
    public static final int BITS_PER_WORD = 64;
    public static final int BYTES_PER_WORD = 8;
    public static final int POINTER_SIZE_IN_WORDS = 1;
    public static final int WORDS_PER_POINTER = 1;

    private static final Logger logger = Logger.getLogger(SerializerUtil.class);

    public static long bytesToLong(byte[] buf) {
        return bytesToLong(buf, 0);
    }
    
    public static long bytesToLong(byte[] data, int offset) {
        long l = 0;
        l |= ((long)(0xff & data[offset + 7])) << 56;
        l |= ((long)(0xff & data[offset + 6])) << 48;
        l |= ((long)(0xff & data[offset + 5])) << 40;
        l |= ((long)(0xff & data[offset + 4])) << 32;
        l |= ((long)(0xff & data[offset + 3])) << 24;
        l |= ((long)(0xff & data[offset + 2])) << 16;
        l |= ((long)(0xff & data[offset + 1])) << 8;
        l |= ((long)(0xff & data[offset]));
        
        return l;
    }

    public static byte[] longToBytes(long l) {
        byte[] buf = new byte[8];
        longToBytes(l, buf, 0);
        return buf;
    }
    
    public static void longToBytes(long l, byte[] buf, int offset) {
        buf[offset] = (byte)l;
        buf[offset + 1] = (byte)(l >> 8);
        buf[offset + 2] = (byte)(l >> 16);
        buf[offset + 3] = (byte)(l >> 24);
        buf[offset + 4] = (byte)(l >> 32);
        buf[offset + 5] = (byte)(l >> 40);
        buf[offset + 6] = (byte)(l >> 48);
        buf[offset + 7] = (byte)(l >> 56);
    }
    
    public static byte[] intToBytes(int n) {
        byte[] buf = new byte[4];
        intToBytes(n, buf, 0);
        return buf;
    }

    public static int bytesToInt(byte[] buf) {
        return bytesToInt(buf, 0);
    }

    public static int bytesToInt(byte[] buf, int offset) {
        int n = 0;
        n |= ((int)(0xff & buf[offset+3])) << 24;
        n |= ((int)(0xff & buf[offset+2])) << 16;
        n |= ((int)(0xff & buf[offset+1])) << 8;
        n |= ((int)(0xff & buf[offset]));
        
        return n;
    }

    public static void intToBytes(int n, byte[] buf, int offset) {
        buf[offset] = (byte)n;
        buf[offset+1] = (byte)(n >> 8);
        buf[offset+2] = (byte)(n >> 16);
        buf[offset+3] = (byte)(n >> 24);
    }

    public static byte[] buildPrimaryKeyOfConnectorMessage(long messageId, int metaDataId) {
        byte[] buf = new byte[12]; // MESSAGE_ID, METADATA_ID
        longToBytes(messageId, buf, 0);
        intToBytes(metaDataId, buf, 8);
        
        return buf;
    }

    public static void writeMessageToEntry(ReusableMessageBuilder rmb, DatabaseEntry data) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(rmb.getInitBufSize());
        ArrayOutputStream output = new ArrayOutputStream(buf);
        SerializePacked.write(output, rmb.getMb());
        buf = output.getWriteBuffer();
        buf.flip();
        data.setData(buf.array(), 0, buf.remaining());
    }

    public static void writeMessageToEntry(ReusableMessageBuilder rmb, ByteBuf buf, DatabaseEntry data) throws IOException {
        //writeMessage(mb, buf);
        //data.setData(buf.array(), buf.readerIndex(), buf.readableBytes());
        writeMessageToEntry(rmb, data);
    }

    public static void writeMessage(MessageBuilder mb, ByteBuf buf) throws IOException {
    
        // taken from org.capnproto.Serialize class
        ByteBuffer[] segments = mb.getSegmentsForOutput();
        int tableSize = (segments.length + 2) & (~1);

        ByteBuffer table = ByteBuffer.allocate(4 * tableSize);
        table.order(ByteOrder.LITTLE_ENDIAN);

        table.putInt(0, segments.length - 1);

        for (int i = 0; i < segments.length; ++i) {
            table.putInt(4 * (i + 1), segments[i].limit() / 8);
        }

        PackedByteBufStream outputChannel = new PackedByteBufStream(buf);
        // Any padding is already zeroed.
        while (table.hasRemaining()) {
            outputChannel.write(table);
        }

        for (ByteBuffer buffer : segments) {
            while(buffer.hasRemaining()) {
                outputChannel.write(buffer);
            }
        }
    }

    public static MessageReader readMessage(byte[] data) throws IOException {
        ArrayInputStream in = new ArrayInputStream(ByteBuffer.wrap(data));
        return SerializePacked.read(in);
    }

    public static MessageReader _readMessage(byte[] data) throws IOException {
        ReaderOptions options = ReaderOptions.DEFAULT_READER_OPTIONS;

        CapnpInputStream in = new CapnpInputStream(data);

        ByteBuffer firstWord = makeByteBuffer(BYTES_PER_WORD);
        fillBuffer(firstWord, in);

        int segmentCount = 1 + firstWord.getInt(0);

        int segment0Size = 0;
        if (segmentCount > 0) {
            segment0Size = firstWord.getInt(4);
        }

        int totalWords = segment0Size;

        if (segmentCount > 512) {
            throw new IOException("too many segments");
        }

        // in words
        ArrayList<Integer> moreSizes = new ArrayList<Integer>();

        if (segmentCount > 1) {
            ByteBuffer moreSizesRaw = makeByteBuffer(4 * (segmentCount & ~1));
            fillBuffer(moreSizesRaw, in);
            for (int ii = 0; ii < segmentCount - 1; ++ii) {
                int size = moreSizesRaw.getInt(ii * 4);
                moreSizes.add(size);
                totalWords += size;
            }
        }

        if (totalWords > options.traversalLimitInWords) {
            throw new DecodeException("Message size exceeds traversal limit.");
        }

        ByteBuffer allSegments = makeByteBuffer(totalWords * BYTES_PER_WORD);
        fillBuffer(allSegments, in);

        ByteBuffer[] segmentSlices = new ByteBuffer[segmentCount];

        allSegments.rewind();
        segmentSlices[0] = allSegments.slice();
        segmentSlices[0].limit(segment0Size * BYTES_PER_WORD);
        segmentSlices[0].order(ByteOrder.LITTLE_ENDIAN);

        int offset = segment0Size;
        for (int ii = 1; ii < segmentCount; ++ii) {
            allSegments.position(offset * BYTES_PER_WORD);
            segmentSlices[ii] = allSegments.slice();
            segmentSlices[ii].limit(moreSizes.get(ii - 1) * BYTES_PER_WORD);
            segmentSlices[ii].order(ByteOrder.LITTLE_ENDIAN);
            offset += moreSizes.get(ii - 1);
        }

        return new MessageReader(segmentSlices, options);
    }
    
    private static ByteBuffer makeByteBuffer(int bytes) {
        ByteBuffer result = ByteBuffer.allocate(bytes);
        result.order(ByteOrder.LITTLE_ENDIAN);
        result.mark();
        return result;
    }

    private static void fillBuffer(ByteBuffer buffer, CapnpInputStream in) throws IOException {
        while(buffer.hasRemaining()) {
            int r = in.read(buffer);
            if (r < 0) {
                throw new IOException("premature EOF");
            }
            // TODO check for r == 0 ?.
        }
    }

    public static ContentType fromCapContentType(CapMessageContent.CapContentType cct) {
        ContentType contentType = null;
        switch (cct) {
        case CHANNELMAP:
            contentType = ContentType.CHANNEL_MAP;
            break;
        case CONNECTORMAP:
            contentType = ContentType.CONNECTOR_MAP;
            break;
        case ENCODED:
            contentType = ContentType.ENCODED;
            break;
        case POSTPROCESSORERROR:
            contentType = ContentType.POSTPROCESSOR_ERROR;
            break;
        case PROCESSEDRAW:
            contentType = ContentType.PROCESSED_RAW;
            break;
        case PROCESSEDRESPONSE:
            contentType = ContentType.PROCESSED_RESPONSE;
            break;
        case PROCESSINGERROR:
            contentType = ContentType.PROCESSING_ERROR;
            break;
        case RAW:
            contentType = ContentType.RAW;
            break;
        case RESPONSE:
            contentType = ContentType.RESPONSE;
            break;
        case RESPONSEERROR:
            contentType = ContentType.RESPONSE_ERROR;
            break;
        case RESPONSEMAP:
            contentType = ContentType.RESPONSE_MAP;
            break;
        case RESPONSETRANSFORMED:
            contentType = ContentType.RESPONSE_TRANSFORMED;
            break;
        case SENT:
            contentType = ContentType.SENT;
            break;
        case SOURCEMAP:
            contentType = ContentType.SOURCE_MAP;
            break;
        case TRANSFORMED:
            contentType = ContentType.TRANSFORMED;
            break;
    
        default:
            throw new IllegalArgumentException("unknown Cap content type " + cct);
        }
        
        return contentType;
    }

    public static byte[] buildPrimaryKeyOfMessageContent(long messageId, int metaDataId, ContentType ct) {
        byte[] buf = new byte[16]; // MESSAGE_ID, METADATA_ID, CONTENT_TYPE
        longToBytes(messageId, buf, 0);
        intToBytes(metaDataId, buf, 8);
        intToBytes(ct.getContentTypeCode(), buf, 12);
        
        return buf;
    }

    public static byte[] serializeProps(Properties props) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream objOut = new ObjectOutputStream(out);
            objOut.writeObject(props);

            return out.toByteArray();
        }
        catch(Exception e) {
            logger.warn("failed to serialize props", e);
        }

        return null;
    }

    public static Properties deserializeProps(byte[] data) {
        try {
            return (Properties) new ObjectInputStream(new ByteArrayInputStream(data)).readObject();
        }
        catch(Exception e) {
            logger.warn("failed to deserialize props", e);
        }

        return null;
    }
}
