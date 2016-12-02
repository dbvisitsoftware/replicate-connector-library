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
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.GregorianCalendar;

import javax.sql.rowset.serial.SerialBlob;

/**
 * Column data value decoder utility class. <p>Provide ability for decoding 
 * supported data types from PLOG tag data chunks</p>
 */
public class ColumnDataDecoder extends DataDecoder {
    /** Chunk offset of century byte in encoded date */
    private static final int DATE_CENTURY_BYTE   = PLOG_CHUNK_BYTES;
    /** Chunk offset of year byte in encoded date */
    private static final int DATE_YEAR_BYTE      = PLOG_CHUNK_BYTES + 1;
    /** Chunk offset of month byte in encoded date */
    private static final int DATE_MONTH_BYTE     = PLOG_CHUNK_BYTES + 2;
    /** Chunk offset of day byte in encoded date */
    private static final int DATE_DAY_BYTE       = PLOG_CHUNK_BYTES + 3;
    /** Chunk offset of hours byte in encoded date */
    private static final int DATE_HOUR_BYTE      = PLOG_CHUNK_BYTES + 4;
    /** Chunk offset of minutes byte in encoded date */
    private static final int DATE_MINUTE_BYTE    = PLOG_CHUNK_BYTES + 5;
    /** Chunk offset of seconds byte in encoded date */
    private static final int DATE_SECOND_BYTE    = PLOG_CHUNK_BYTES + 6;
    /** Minimum allowed size of date in chunks */
    private static final int DATE_MIN_SIZE_BYTES = PLOG_CHUNK_BYTES + 7;

    /**
     * Decode UTF-8 string field from PLOG tag data chunks
     * 
     * @param rawData PLOG tag data chunk array
     * 
     * @return        Decoded UTF-8 string value.
     * @throws        Exception Failed to decode the raw PLOG value
     */
    public static String decodeString (int[] rawData) throws Exception {
        String str = null;
        ByteBuffer b = decodeByteBuffer (rawData);

        /* first chunk is string length; data start at second chunk,,
         * buffer is advanced by PLOG_CHUNK_BYTES 
         */
        int rawLength = b.getInt();

        byte[] strBytes = new byte[rawLength];

        b.get(strBytes, 0, rawLength);

        try {
            str = new String(strBytes, PLOG_CHUNK_STRING_ENC);
        }
        catch (UnsupportedEncodingException e) {
            throw new Exception (
                "Invalid data chunk array, reason: " + e.getMessage()
            );
        }

        return str;
    }

    /**
     * A very simple, and not fail proof, check to determine if the national
     * character set in the source could have been UTF-16.
     * 
     * <p>
     * This is a work around for not having the character set used by 
     * source database as the national character set encoded in the PLOG
     * </p>
     * 
     * @param bytes raw bytes
     * 
     * @return true if the national character set could have been UTF16,
     *         else false
     */
    public static boolean couldBeUTF16 (byte[] bytes) {
        boolean check;
        
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
        decoder.onMalformedInput(CodingErrorAction.REPORT);
        decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
        
        try {
            String s = decoder.decode(buffer).toString();
            
            if (!s.contains("\u0000")) {
                /* could be utf-16 */
                check = false;
            }
            else {
                check = true;
            }
        } catch (CharacterCodingException e) {
            /* error in utf-8, use utf-16 */
            check = true;
        }
        return check;
    }
    
    /**
     * Attempt to decode national string as UTF-16 if we guess it could be
     * the national character set, else default to using UTF-8.
     * 
     * @param rawData PLOG tag pay load of chunk arrays
     * 
     * @return National string decoded as UTF-8
     * 
     * @throws Exception when failed to decode national string to UTF-8
     */
    public static String decodeNationalString (int[] rawData)
    throws Exception {
        String str   = null;
        ByteBuffer b = decodeByteBuffer (rawData);
        
        /* first chunk is string length; data start at second chunk,,
         * buffer is advanced by PLOG_CHUNK_BYTES 
         */
        int rawLength = b.getInt();
        
        byte[] strBytes = new byte[rawLength];
        
        b.get(strBytes, 0, rawLength);
        
        if (couldBeUTF16 (strBytes)) {
            try {
                /* interpret as UTF-16 */
                String nstr = new String(
                    strBytes,
                    PLOG_CHUNK_NAT_STRING_ENC
                );
                
                /* use UTF-16 version instead as UTF-8 */
                str = new String (nstr.getBytes(PLOG_CHUNK_STRING_ENC));
            }
            catch (UnsupportedEncodingException e) {
                throw new Exception (
                    "Invalid data chunk array for national string, reason: " + 
                    e.getMessage()
                );
            }
        }
        else {
            try {
                str = new String(strBytes, PLOG_CHUNK_STRING_ENC);
            }
            catch (UnsupportedEncodingException e) {
                throw new Exception (
                    "Invalid data chunk array, reason: " + e.getMessage()
                );
            }
        }
        
        return str;
    }
    
    /**
     * Decode number field in PLOG as big decimal with the scale provided
     * 
     * @param rawData PLOG tag data chunk array
     * @param scale   Scale to apply for output big decimal value, provided
     *                from field metadata
     *
     * @return        Decoded big decimal value
     * @throws        Exception Failed to decode the raw PLOG value
     */
    public static BigDecimal decodeNumber (int[] rawData, int scale) 
    throws Exception {
        BigDecimal value = null;
        ByteBuffer b = decodeByteBuffer (rawData);

        /* use raw byte array copy for direct offset decoding below */
        byte[] barr = b.array();

        /* first int is value length; data start at barr[4] */
        int rawLength = b.getInt();

        if (rawLength == 0) {
            value = null;
        }
        else if (rawLength == 1 && ((int)barr[PLOG_CHUNK_BYTES] & 0xFF) == 0x80) 
        {
            /* "80" is 0 */
            value = (new BigDecimal(0)).setScale(scale);
        }
        else {
            int length;
            int shift;
            StringBuilder rtval = new StringBuilder();
            if (barr[PLOG_CHUNK_BYTES + rawLength - 1] == PLOG_NEGATIVE_NUMBER) 
            { 
                /* last byte 0x66 = negative number */
                length = rawLength - 1;

                /* 1 = /100, -1=*100 (reference decimal point: after first 
                 * digit pair)
                 */
                shift = -(((int) barr[PLOG_CHUNK_BYTES] & 0xFF) - 0x3e);
                rtval.append("-.");
                for (int o = 1; o < length; o++) {
                    rtval.append(
                        String.format(
                            "%02d",
                            101 - ((int) barr[PLOG_CHUNK_BYTES + o] & 0xFF)
                        )
                    );
                }
            }
            else {
                length = rawLength;
                rtval.append(".");

                /* the (int)&0xFF is to force unsigned treatment */
                shift = ((int) barr[PLOG_CHUNK_BYTES] & 0xFF) - 0xc1; 

                for (int o = 1; o < length; o++) {
                    rtval.append(
                        String.format(
                            "%02d",
                            ((int) barr[PLOG_CHUNK_BYTES + o] & 0xFF) - 1
                        )
                    );
                }
            }

            BigDecimal bd = new BigDecimal(rtval.toString());

            /* The decoded scale value is offset by +2 because the 
             * decimal is encoded differently than in Oracle's 
             * format
             */
            bd = bd.scaleByPowerOfTen(shift * 2 + 2);

            BigDecimal v = 
                new BigDecimal (
                    bd.toPlainString()
                ).setScale (scale, BigDecimal.ROUND_HALF_DOWN);

            value = v;
        }

        return value;
    }

    /**
     * Decode a raw PLOG number field with no scale as 32 bit integer data type
     * 
     * @param rawData PLOG tag data chunk array
     * @param scale   Scale for integral, either &le; 0
     * 
     * @return        Decoded integer value
     * @throws        Exception Failed to decode the raw PLOG value
     */
    public static Integer decodeNumberAsInt (int[] rawData, int scale) 
    throws Exception {
        if (scale > 0) {
            throw new Exception (
                "Unable to decode integral number with scale: " + scale
            );
        }

        BigDecimal bi = null;
        Integer i = null;

        try {
            bi = decodeNumber (rawData, scale);

            if (bi != null) {
                i = bi.intValueExact();
            }
        }
        catch (Exception e) {
            throw new Exception (
                "Failed to decode raw number:" + bi + " with scale: " + 
                scale + " as integer, reason: " + e.getMessage()
            );
        }

        return i;
    }

    /**
     * Decode a raw PLOG number field with no scale as 64 bit integer data type
     * 
     * @param rawData PLOG tag data chunk array
     * @param scale   Scale for integral, either &le; 0
     * 
     * @return        Decoded long value
     * @throws        Exception Failed to decode the raw PLOG value
     */
    public static Long decodeNumberAsLong (int[] rawData, int scale) 
    throws Exception {
        if (scale > 0) {
            throw new Exception (
                "Unable to decode long integral number with scale: " + scale
            );
        }

        BigDecimal bl = null;
        Long l = null;

        try {
            bl = decodeNumber (rawData, scale);

            if (bl != null) {
                l = bl.longValueExact();
            }
        }
        catch (Exception e) {
            throw new Exception (
                "Failed to decode raw number: " + bl + " with scale: " + 
                scale + " as long, reason: " + e.getMessage()
            );
        }

        return l;
    }

    /**
     * Decode a raw binary column from PLOG tag data chunks, do not use
     * for CLOBs or BLOBs
     * 
     * @param rawData PLOG tag data chunk array
     * 
     * @return        Decoded binary object as SerialBlob
     * @throws        Exception Failed to decode the raw PLOG value
     */
    public static SerialBlob decodeBinary (int[] rawData) throws Exception {
        SerialBlob blob = null;
        ByteBuffer b = decodeByteBuffer (rawData);

        /* first chunk is raw length; data start at second chunk,
         * buffer is advanced by PLOG_CHUNK_BYTES 
         */
        int rawLength = b.getInt();

        byte[] raw = new byte[rawLength];

        /* read from current byte to end */
        b.get(raw, 0, rawLength);

        try {
            blob = new SerialBlob(raw);
        }
        catch (Exception e) {
            throw new Exception (
                "Invalid data chunk array, reason: " + e.getMessage()
            );
        }

        return blob;
    }

    /**
     * Decode date column from PLOG tag data chunks
     * 
     * @param rawData PLOG tag data chunk array
     * 
     * @return        Date as time stamp
     * @throws        Exception Failed to decode the raw PLOG value
     */
    public static Timestamp decodeDate (int[] rawData) throws Exception {
        ByteBuffer b = decodeByteBuffer (rawData);
        Timestamp t  = null;

        /* first chunk is raw length; data start at second chunk,
         * buffer is advanced by PLOG_CHUNK_BYTES 
         */
        b.getInt();

        /* use raw byte array copy for direct offset decoding below */
        byte[] barr = b.array();

        if (barr.length < DATE_MIN_SIZE_BYTES) {
            /* NULL date object */
            t = null;
        }
        else {
            /* 0: 100-offset century
             * 1: 100-offset year
             * 2: 0-offset month, but -1, January is 0
             * 3: 0-offset day
             * 4: 1-offset hour
             * 5: 1-offset minute
             * 6: 1-offset second
             */
            GregorianCalendar gcal = new GregorianCalendar(
                (((int) barr[DATE_CENTURY_BYTE] & 0xFF) - 100) * 100
                    + (((int) barr[DATE_YEAR_BYTE] & 0xFF) - 100),
                barr[DATE_MONTH_BYTE] - 1,
                barr[DATE_DAY_BYTE],
                barr[DATE_HOUR_BYTE] - 1,
                barr[DATE_MINUTE_BYTE] - 1,
                barr[DATE_SECOND_BYTE] - 1
            );

            t = new Timestamp(gcal.getTimeInMillis());
        }

        return t;
    }

    /**
     * Decode timestamp column value from PLOG tag data chunks
     * 
     * @param rawData PLOG tag data chunk array
     * 
     * @return        Time stamp
     * @throws        Exception Failed to decode the raw PLOG value
     */
    public static Timestamp decodeTimestamp (int[] rawData) throws Exception {
        Timestamp ts = null;
        ByteBuffer b = decodeByteBuffer (rawData);

        /* first chunk is raw length; data start at second chunk,
         * buffer is advanced by PLOG_CHUNK_BYTES 
         */
        int rawLength = b.getInt();

        /* use raw byte array copy for direct offset decoding below */
        byte[] barr = b.array();

        if (barr.length < DATE_MIN_SIZE_BYTES) {
            /* NULL */
            ts = null;
        }
        else {
            /* 0: 100-offset century
             * 1: 100-offset year
             * 2: 0-offset month, but -1, January is 0
             * 3: 0-offset day
             * 4: 1-offset hour
             * 5: 1-offset minute
             * 6: 1-offset second
             */
            GregorianCalendar gcal = new GregorianCalendar(
                (((int) barr[DATE_CENTURY_BYTE] & 0xFF) - 100) * 100
                    + (((int) barr[DATE_YEAR_BYTE] & 0xFF) - 100),
                barr[DATE_MONTH_BYTE] - 1,
                barr[DATE_DAY_BYTE],
                barr[DATE_HOUR_BYTE] - 1,
                barr[DATE_MINUTE_BYTE] - 1,
                barr[DATE_SECOND_BYTE] - 1
            );

            int umilli = 0;
            for (int j = 7; j < rawLength; j++) {
                umilli = (umilli << 8) + ((int) barr[PLOG_CHUNK_BYTES + j] & 0xFF);
            }

            if (umilli > 999999999) {
                /* truncate nano seconds to supported precision */
                umilli = 999999999;
            }
            if (umilli < 0) {
                umilli = 0;
            }

            ts = new Timestamp(gcal.getTimeInMillis());
            ts.setNanos(umilli);

            /*
             * WARNING: Calendar does not support enough precision!!!;
             * java.sql.timestamp does not have time zone information, we
             * expect to have always UTC from mine (MINE_CONVERT_TZ_TO_UTC =
             * YES)
             */
        }

        return ts;
    }

    /**
     * Decode timestamp with time zone data
     * 
     * @param rawData PLOG tag data chunk array
     * 
     * @return        Time stamp with time zone.
     * @throws        Exception Failed to decode the raw PLOG value
     */
    public static Timestamp decodeTimestampWithTz (int[] rawData) 
    throws Exception 
    {
        Timestamp ts = null;
        ByteBuffer b = decodeByteBuffer (rawData);

        /* first chunk is raw length; data start at second chunk,
         * buffer is advanced by PLOG_CHUNK_BYTES 
         */
        int rawLength = b.getInt();

        /* use raw byte array copy for direct offset decoding below */
        byte[] barr = b.array();

        if (barr.length < DATE_MIN_SIZE_BYTES) {
            /* NULL */
            ts = null;
        }
        else {
            /* 0: 100-offset century
             * 1: 100-offset year
             * 2: 0-offset month, but -1, January is 0
             * 3: 0-offset day
             * 4: 1-offset hour
             * 5: 1-offset minute
             * 6: 1-offset second
             */
            GregorianCalendar gcal = new GregorianCalendar(
                (((int) barr[DATE_CENTURY_BYTE] & 0xFF) - 100) * 100
                    + (((int) barr[DATE_YEAR_BYTE] & 0xFF) - 100), 
                barr[DATE_MONTH_BYTE] - 1, 
                barr[DATE_DAY_BYTE],
                barr[DATE_HOUR_BYTE] - 1,
                barr[DATE_MINUTE_BYTE] - 1,
                barr[DATE_SECOND_BYTE] - 1
            );

            int umilli = 0;
            for (int j = 7; j < rawLength - 2; j++) {
                umilli = (umilli << 8) + ((int) barr[PLOG_CHUNK_BYTES + j] & 0xFF);
            }
            int tzh = ((int) barr[PLOG_CHUNK_BYTES + rawLength - 2] & 0xFF);
            int tzm = ((int) barr[PLOG_CHUNK_BYTES + rawLength - 1] & 0xFF);

            if (tzh != 0xd0 || tzm != 0x4) {
                logger.warn(
                    "Timezone was not UTC in TIMESTAMP WITH TIME ZONE (tzh=" +
                    tzh + ", tzm=" + tzm + ")"
                );
            }

            ts = new Timestamp(gcal.getTimeInMillis());
            ts.setNanos(umilli);
        }

        return ts;
    }

    /**
     * Decode time stamp with local time zone
     * 
     * @param rawData PLOG tag data chunk array
     * 
     * @return        Decoded time stamp
     * @throws        Exception Failed to decode the raw PLOG value
     */
    public static Timestamp decodeTimestampWithLocalTz (int[] rawData) 
    throws Exception 
    {
        return decodeTimestamp (rawData);
    }

    /**
     * Decode interval day to second column as string value
     * 
     * @param rawData PLOG tag data chunk array
     * 
     * @return        Formatted interval string
     * @throws        Exception Failed to decode the raw PLOG value
     */
    public static String decodeIntervalDayToSec (int[] rawData) 
    throws Exception 
    {
        String interval = null;
        ByteBuffer b = decodeByteBuffer (rawData);

        /* first chunk is raw length; data start at second chunk,
         * buffer is advanced by PLOG_CHUNK_BYTES 
         */
        b.getInt();

        /* use raw byte array copy for direct offset decoding below */
        byte[] barr = b.array();

        long d = 0;
        for (int j = 0; j < 4; j++) {
            d = (d << 8) + ((int) barr[PLOG_CHUNK_BYTES + j] & 0xFF);
        }
        d -= 0x80000000L;

        int h = ((int) barr[PLOG_CHUNK_BYTES + 4] & 0xFF) - 60;
        int mi = ((int) barr[PLOG_CHUNK_BYTES + 5] & 0xFF) - 60;
        int s = ((int) barr[PLOG_CHUNK_BYTES + 6] & 0xFF) - 60;

        long umilli = 0;
        for (int j = 7; j < 11; j++) {
            umilli = (umilli << 8) + ((int) barr[PLOG_CHUNK_BYTES + j] & 0xFF);
        }
        umilli -= 0x80000000L;

        interval = String.format(
            "%+d %d:%d:%d.%05d", 
            d,
            Math.abs(h), 
            Math.abs(mi), 
            Math.abs(s),
            Math.abs(umilli)
        );

        return interval;
    }

    /**
     * Decode interval year to month column as string value
     * 
     * @param rawData PLOG tag data chunk array
     * 
     * @return        Formatted interval string
     * @throws        Exception Failed to decode the raw PLOG value
     */
    public static String decodeIntervalYearToMonth (int[] rawData) 
    throws Exception 
    {
        String interval = null;
        ByteBuffer b = decodeByteBuffer (rawData);

        /* first chunk is raw length; data start at second chunk,
         * buffer is advanced by PLOG_CHUNK_BYTES 
         */
        b.getInt();

        /* use raw byte array copy for direct offset decoding below */
        byte[] barr = b.array();

        long y = 0;
        for (int j = 0; j < 4; j++) {
            y = (y << 8) + ((int) barr[PLOG_CHUNK_BYTES + j] & 0xFF);
        }
        y -= 0x80000000L;

        int m = ((int) barr[PLOG_CHUNK_BYTES + 4] & 0xFF) - 60;

        interval = String.format("%+d-%d", y, Math.abs(m));

        return interval;
    }

}
