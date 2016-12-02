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
import java.nio.LongBuffer;
import java.sql.Timestamp;
import java.util.GregorianCalendar;

/**
 * Simple data value decoder utility class. <p>Provide ability for decoding simple
 * data fields, eg. fixed size and null-terminated char fields</p>
 */
public class SimpleDataDecoder extends DataDecoder {
    /** 
     * Decode integer from PLOG tag data chunks, use to convert tag value to an 
     * 32 bit integer.
     * 
     * @param rawData PLOG tag data chunk array
     * 
     * @return        Decoded integer value
     * @throws        Exception Failed to decode the raw PLOG value
     */
    public static Integer decodeInteger (int[] rawData) throws Exception {
        /* PLOG chunks are 4 byte aligned and a Java primitive integer is 
         * always 4 byte, see Java specification
         */
        if (rawData.length < PLOG_CHUNK_INT_LEN) {
            throw new Exception (
                "Invalid data chunk array, expect at least " +
                PLOG_CHUNK_INT_LEN + " chunks"
            );
        }
        return rawData[0];
    }

    /**
     * Decode long value from PLOG tag data chunks, use to convert tag value to a 
     * 64-bit long. 
     * 
     * @param rawData PLOG tag data chunk array
     * 
     * @return        Decoded Long value
     * @throws        Exception Failed to decode the raw PLOG value
     */
    public static Long decodeLong (int[] rawData) throws Exception {
        if (rawData.length < PLOG_CHUNK_LONG_LEN) {
            throw new Exception (
                "Invalid data chunk array, expect at least " +
                PLOG_CHUNK_LONG_LEN + " chunks"
            );
        }
        LongBuffer l =
            decodeByteBuffer (rawData, PLOG_CHUNK_LONG_LEN).asLongBuffer();

        return l.get();
    }

    /**
     * Decode null-terminated string from PLOG tag data chunks, use to convert 
     * tag value to a string. 
     * 
     * For now it assumes US-ASCII and thus does not support non-ASCII in 
     * column/table names
     * 
     * @param rawData PLOG tag data chunk array
     * 
     * @return        Decoded string value
     * @throws        Exception Failed to decode the raw PLOG value
     */
    public static String decodeCharString (int[] rawData) throws Exception {
        String str = null;
        ByteBuffer b = decodeByteBuffer (rawData);

        try {
            str = new String(b.array(), PLOG_CHUNK_CHAR_STRING_ENC); 
        }
        catch (java.io.UnsupportedEncodingException e) {
            throw new Exception (
                "Invalid data chunk array, reason: " + e.getMessage()
            );
        }

        /* return string without trailing NULL and anything further */
        return str.substring(0, str.indexOf("\0")); 
    }

    /**
     * Decode date from PLOG tag data chunks, use to convert tag value to 
     * a time stamp. Source is Oracle internal time representation as used
     * in redo headers etc.
     * 
     * @param rawData PLOG tag data chunk array
     * 
     * @return        Decoded date as time stamp value
     * @throws        Exception Failed to decode the raw PLOG value
     */
    public static Timestamp decodeDate (int[] rawData) throws Exception {
        int t, seconds, minutes, hours, day, month, year;

        t = rawData[0];
        seconds = t % 60;
        t /= 60;
        minutes = t % 60;
        t /= 60;
        hours = t % 24;
        t /= 24;
        day = t % 31 + 1;
        t /= 31;
        month = t % 12 + 1;
        t /= 12;
        year = t + 1988;

        GregorianCalendar gc = new GregorianCalendar(
            year, 
            month - 1 ,
            day,
            hours,
            minutes,
            seconds
        );

        return new Timestamp(gc.getTimeInMillis());
    }

}
