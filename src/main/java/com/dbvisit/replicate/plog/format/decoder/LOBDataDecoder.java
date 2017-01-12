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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import javax.sql.rowset.serial.SerialBlob;

/**
 * LOB column data value decoder utility class. <p>Provide ability for decoding 
 * CLOB and BLOB data types from 4 byte aligned PLOG tag data chunks</p>
 */
public class LOBDataDecoder extends DataDecoder {
    /**
     * Decode Character Large Object column from PLOG tag data chunk, empty
     * and NULL CLOBs are supported
     * 
     * @param rawData   PLOG tag data chunk array
     * @param lobLength The actual length of LOB as parsed in TAG_LOBLEN tag
     * 
     * @return          Decoded CLOB as String value, an empty String is 
     *                  returned for zero length CLOBs and a NULL when
     *                  source CLOB was NULL
     * @throws          Exception Failed to decode the raw PLOG value
     */
    public static String decodeCLOB (
        int[] rawData,
        long  lobLength
    )
    throws Exception
    {
        String clob = null;
        
        if (rawData.length == 0) {
            /* no bytes to decode for CLOB, it is NULL */
            clob = null;
        }
        else {
            /* have bytes to decode as byte buffer */
            ByteBuffer b = decodeByteBuffer (rawData);
    
            /* first chunk is raw length; data start at second chunk,
             * buffer is advanced by PLOG_CHUNK_BYTES 
             */
            int rawLength = b.getInt();
            
            if (rawLength == 0) {
                /* length of encoded CLOB is zero, nothing to decode */
                clob = "";
            }
            else if (rawLength > 0) {
                /* upper 32-bit of length, ignore for now, we handle chunks <4GB
                 * only */
                b.getInt();
        
                int actualLength;
        
                if (lobLength > 0      && 
                    lobLength % 2 == 0 && 
                    (int)(lobLength >> 1) == rawLength) 
                {
                    /* LOB length encoded in bytes, if source CLOB was stored in
                     * UTF16 we need to adjust length
                     */
                    actualLength = (int)(lobLength >> 1);
                }
                else if (lobLength > 0) { 
                    /* CLOB = 8-bit on source */
                    actualLength = (int)lobLength;
                }
                else {
                    actualLength = rawLength;
                }
        
                if (logger.isTraceEnabled()) {
                    logger.trace(bytesToHex(b.array()));
                    logger.trace(
                        "Actual length:" + actualLength + " " + 
                        "CLOB length:"   + lobLength    + " " + 
                        "Raw length:"    + rawLength
                    );
                }
        
                byte[] raw = new byte[rawLength];
                b.get(raw, 0, rawLength);
        
                try {
                    clob = new String(
                        raw, 
                        PLOG_CHUNK_STRING_ENC
                    ).substring(0, actualLength);
                }
                catch (UnsupportedEncodingException e) {
                    throw new Exception (
                        "Invalid CLOB data chunk, reason: " + e.getMessage()
                    );
                }
            }
            else {
                /* not a valid length */
                throw new Exception (
                    "Invalid CLOB data chunk, reason: encoded length=" +
                    rawLength + " is invalid"
                );
            }
        }
            
        return clob;
    }

    /**
     * Decode Binary Large Object column value in PLOG tag data chunk,
     * empty and NULL BLOBs are supported
     * 
     * @param rawData   PLOG tag data chunk array
     * @param lobLength The actual length of LOB as parsed in TAG_LOBLEN tag
     * 
     * @return          decoded as SerialBlob, an empty object is returned for
     *                  zero length BLOBs and a NULL objects for NULL BLOBs
     * @throws          Exception Failed to decode the raw PLOG value
     */
    public static SerialBlob decodeBLOB (int[] rawData, long lobLength) 
    throws Exception 
    {
        SerialBlob blob = null;
        
        if (rawData.length == 0) {
            /* no bytes to decode for BLOB, it is NULL */
            blob = null;
        }
        else {
            /* have bytes to decode as byte buffer */
            ByteBuffer b = decodeByteBuffer (rawData);
            
            /* first chunk is raw length; data start at second chunk,
             * buffer is advanced by PLOG_CHUNK_BYTES 
             */
            int rawLength = b.getInt();
    
            if (rawLength == 0) {
                /* length of encoded BLOB is zero, nothing to decode */
                blob = new SerialBlob (new byte[] {});
            }
            else if (rawLength > 0) {
                /* upper 32-bit of length, ignore for now, we handle chunks 
                 * <4GB only */
                b.getInt();
                
                /* actual length of BLOB */
                int actualLength = lobLength > 0 
                    ? (int)lobLength 
                    : rawLength;
        
                if (logger.isTraceEnabled()) {
                    logger.trace(bytesToHex(b.array()));
                    logger.trace(
                        "Actual length:" + actualLength + " " + 
                        "BLOB length:"   + lobLength    + " " + 
                        "Raw length:"    + rawLength
                    );
                }
        
                byte[] raw = new byte[actualLength];
                b.get(raw, 0, actualLength);
        
                try {
                    blob = new SerialBlob(raw);
                }
                catch (Exception e) {
                    throw new Exception (
                        "Invalid BLOB data chunk, reason: " + e.getMessage()
                    );
                }
            }
            else {
                /* not a valid length */
                throw new Exception (
                    "Invalid BLOB data chunk, reason: encoded length=" +
                    rawLength + " is invalid"
                );
            }
        }
        
        return blob;
    }

}
