package com.dbvisit.replicate.plog.format.decoder;

/**
 * Copyright 2016 Dbvisit Software Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all decoder utility classes. <p>Provide functions to decode 
 * raw PLOG tag data chunks</p>
 */
public class DataDecoder {
    protected static final Logger logger = LoggerFactory.getLogger(
            DataDecoder.class
    );

    /** Size of PLOG chunk in bytes */
    final protected static int PLOG_CHUNK_BYTES    = 4;
    /** Number of PLOG chunks used to encode integer */
    final protected static int PLOG_CHUNK_INT_LEN  = 1; 
    /** Number of PLOG chunks used to encode long value */
    final protected static int PLOG_CHUNK_LONG_LEN = 2;
    /** Default character set encoding for null terminated strings in PLOG */
    final protected static String PLOG_CHUNK_CHAR_STRING_ENC = "US-ASCII";
    /** Default character set encoding for String chunks in PLOG */
    final protected static String PLOG_CHUNK_STRING_ENC      = "UTF-8";
    /** The default character set encoding for national character set */
    final protected static String PLOG_CHUNK_NAT_STRING_ENC  = "UTF-16";
    /** Valued encoded for negative number in PLOG */
    final protected static int PLOG_NEGATIVE_NUMBER = 0x66;
    /** Help to convert encoded bytes as readable hexadecimal string */
    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();
    /** Precision used to decode Oracle integer numbers as integral value */
    final public static int NUMBER_INTEGER_MAX_PRECISION = 10;
    /** Precision used to decode Oracle long numbers as long integral value */
    final public static int NUMBER_LONG_MAX_PRECISION = 20;

    /**
     * Helper function to decode number of PLOG chunks as byte buffer from 
     * which the data values will be decoded
     *
     * @param rawData   array of PLOG chunks, these are 4 bytes in size each,
     *                  as int
     * @param numChunks the number of PLOG chunk to decode from incoming
     *                  PLOG chunk array
     *
     * @return byte buffer in LITTLE ENDIAN byte order
     * @throws Exception for a buffer overrun or decode error
     */
    protected static ByteBuffer decodeByteBuffer (int[] rawData, int numChunks) 
    throws Exception 
    {
        if (numChunks > rawData.length) {
            throw new Exception ("Internal error, byte buffer overrun");
        }

        byte[] bytes = new byte[rawData.length * PLOG_CHUNK_BYTES];
        ByteBuffer bytebuf = ByteBuffer.wrap(bytes, 0, bytes.length);
        bytebuf.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        IntBuffer intbuf = bytebuf.asIntBuffer();
        intbuf.put(rawData, 0, numChunks);

        return bytebuf;
    }

    /**
     * Helper function to decode complete set of PLOG chunks
     *
     * @param rawData PLOG chunks to decode as bytes
     *
     * @return raw decoded byte data
     * @throws Exception for any decode errors
     */
    protected static ByteBuffer decodeByteBuffer (int[] rawData) 
    throws Exception
    {
        return decodeByteBuffer (rawData, rawData.length);
    }

    /**
     * Convert byte array to hexadecimal string for human-readable logging
     * 
     * @param bytes input byte[] array
     * @return text hex string representation 
     */
    public static String bytesToHex(byte[] bytes)
    {
        char[] hexChars = new char[bytes.length * 2];

        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

}
