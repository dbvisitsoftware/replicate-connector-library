package com.dbvisit.replicate.plog.format.decoder;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import javax.sql.rowset.serial.SerialBlob;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Simple tests for validating decoding of the raw data payload of
 *  entry tag records in PLOG 
 */
public class ColumnDataDecoderTest {
    private static final Logger logger = LoggerFactory.getLogger(
        ColumnDataDecoderTest.class
    );
    
    @Test
    public void testDecodeNumberAsInt() {
        /* UNITTEST SQL:
        
           CREATE TABLE SOE.UNITTEST (TEST NUMBER(4));
           INSERT INTO SOE.UNITTEST VALUES (9999);
        */
        final int []INT_NUMBER = {
            3, 6579394
        };
        final int EXPECTED = 9999;
        final int SCALE = 0;
        
        try {
            int decoded = ColumnDataDecoder.decodeNumberAsInt(
                INT_NUMBER,
                SCALE
            );
            
            logger.info ("Decoded Integer: " + decoded);
            
            assertTrue (
                "Expecting Integer: " + EXPECTED + ", got: " + decoded,
                decoded == EXPECTED
            );
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testDecodeNumberAsLong() {
        /* UNITTEST SQL:
        
           CREATE TABLE SOE.UNITTEST (TEST NUMBER(18));
           INSERT INTO SOE.UNITTEST VALUES (99999999999999999);
        */
        final int []LONG_NUMBER = {
            10, 1684277961, 1684300900, 25700
        };
        final long EXPECTED = 99999999999999999L;
        final int SCALE = 0;
        
        try {
            long decoded = ColumnDataDecoder.decodeNumberAsLong(
                LONG_NUMBER,
                SCALE
            );
            
            logger.info ("Decoded Long: " + decoded);
            
            assertTrue (
                "Expecting Long: " + EXPECTED + ", got: " + decoded,
                decoded == EXPECTED
            );
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testDecodeNumber() {
        /* UNITTEST SQL:
            
           CREATE TABLE SOE.UNITTEST (TEST NUMBER(12, 5));
           INSERT INTO SOE.UNITTEST VALUES ('9999999.99999');
         */
        final int []DECIMAL_NUMBER = {
            8, 1684277956, 1533305956
        };
        final BigDecimal EXPECTED = new BigDecimal("9999999.99999");
        final int SCALE = 5;
        
        try {
            BigDecimal decoded = ColumnDataDecoder.decodeNumber(
                DECIMAL_NUMBER,
                SCALE
            );
            
            logger.info ("Decoded BigDecimal: " + decoded);
            
            assertTrue (
                "Expecting BigDecimal: " + EXPECTED + ", got: " + decoded,
                decoded.equals(EXPECTED)
            );
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testDecodeString() {
        /* UNITTEST SQL:

           CREATE TABLE SOE.UNITTEST (TEST VARCHAR2(255));
           INSERT INTO SOE.UNITTEST VALUES ('Test character string to decode - 0123456789 - ~!@#$%^&*()_+');
           COMMIT;
        */
        final int []VARCHAR2 = {
            60, 1953719636, 1634231072, 1952670066, 1931506277,
            1852404340, 1869881447, 1667589152, 543515759, 825237549,
            892613426, 959985462, 2116037920, 606289953, 707157541,
            727656744
        };
        final String EXPECTED = 
            "Test character string to decode - 0123456789 - ~!@#$%^&*()_+";
        
        try {
            String decoded = ColumnDataDecoder.decodeString(VARCHAR2);
            
            logger.info ("Decoded Varchar: " + decoded);
            
            assertTrue (
                "Expecting Varchar: " + EXPECTED + ", got: " + decoded,
                decoded.equals(EXPECTED)
            );
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testDecodeNString() {
        /* UNITTEST SQL:

           CREATE TABLE SOE.UNITTEST (TEST NVARCHAR2(255));
           INSERT INTO SOE.UNITTEST VALUES ('Test character string to decode - 0123456789 - ~!@#$%^&*()_+');
           COMMIT;
         */
        final int []NVARCHAR2 = {
            60, 1953719636, 1634231072, 1952670066, 1931506277,
            1852404340, 1869881447, 1667589152, 543515759, 825237549,
            892613426, 959985462, 2116037920, 606289953, 707157541,
            727656744
        };
        final String EXPECTED = 
            "Test character string to decode - 0123456789 - ~!@#$%^&*()_+";
            
        try {
            String decoded = ColumnDataDecoder.decodeString(NVARCHAR2);

            logger.info ("Decoded Nvarchar: " + decoded);

            assertTrue (
                "Expecting Nvarchar: " + EXPECTED + ", got: " + decoded,
                decoded.equals(EXPECTED)
            );
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testDecodeLongString() {
        /* UNITTEST SQL:

           CREATE TABLE SOE.UNITTEST (TEST LONG);
           INSERT INTO SOE.UNITTEST VALUES ('LONG test character string to decode - 0123456789 - ~!@#$%^&*()_+');
           COMMIT;
         */
        final int []LONG_CHAR = {
            65, 1196314444, 1936028704, 1751326836, 1667330657,
            544367988,1769108595, 1948280686, 1701060719, 1701080931,
            807415072, 875770417, 943142453, 539828281, 591405438,
            643704100, 1596532778, 43
        };
        final String EXPECTED = 
            "LONG test character string to decode - 0123456789 - ~!@#$%^&*()_+";
                
        try {
            String decoded = ColumnDataDecoder.decodeString(LONG_CHAR);

            logger.info ("Decoded String: " + decoded);

            assertTrue (
                "Expecting String: " + EXPECTED + ", got: " + decoded,
                decoded.equals(EXPECTED)
            );
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testDecodeBinary() {
        /* UNITTEST SQL:

           CREATE TABLE SOE.UNITTEST (TEST RAW(200));
           INSERT INTO SOE.UNITTEST VALUES ('0102030405060708090A0B0C0D0E0F');
           COMMIT;
         */
        final int []RAW = {
            15, 67305985, 134678021, 202050057, 986637
        };
        final String EXPECTED = "0102030405060708090A0B0C0D0E0F";
                    
        try {
            SerialBlob sb = ColumnDataDecoder.decodeBinary(RAW);
            
            String decoded = 
                DataDecoder.bytesToHex(
                    sb.getBytes(1, (int)sb.length())
                );

            logger.info ("Decoded Binary: " + decoded);

            assertTrue (
                "Expecting String: " + EXPECTED + ", got: " + decoded,
                decoded.equals(EXPECTED)
            );
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testDecodeDate() {
        /* UNITTEST SQL:

           CREATE TABLE SOE.UNITTEST (TEST DATE);
           INSERT INTO SOE.UNITTEST VALUES (TO_DATE('2016/09/01', 'YYYY/MM/DD'));
           COMMIT;
         */
        final int []DATE = {
            7, 17396856, 65793
        };
        
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
        Timestamp EXPECTED = null;
        
        try {
            Date d = dateFormat.parse("2016/09/01");
            EXPECTED = new Timestamp (d.getTime());
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
        
        try {
            Timestamp decoded = ColumnDataDecoder.decodeDate(DATE);
            
            logger.info ("Decoded Date: " + decoded);

            assertTrue (
                "Expecting Date: " + EXPECTED + ", got: " + decoded,
                decoded.equals(EXPECTED)
            );
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testDecodeTimestamp() {
        /* UNITTEST SQL:

           CREATE TABLE SOE.UNITTEST (TEST TIMESTAMP);
           INSERT INTO SOE.UNITTEST VALUES (TO_TIMESTAMP('2016/09/01 00:00:01', 'YYYY/MM/DD HH24:MI:SS'));
           COMMIT;
         */
        final int []TIMESTAMP = {
            7, 17396856, 131329
        };
        
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss");
        Timestamp EXPECTED = null;
        
        try {
            Date d = dateFormat.parse("2016/09/01 00:00:01");
            EXPECTED = new Timestamp (d.getTime());
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
        
        try {
            Timestamp decoded = ColumnDataDecoder.decodeTimestamp(TIMESTAMP);
            
            logger.info ("Decoded Timestamp: " + decoded);

            assertTrue (
                "Expecting Timestamp: " + EXPECTED + ", got: " + decoded,
                decoded.equals(EXPECTED)
            );
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testDecodeTimestampWithTz() {
        /* UNITTEST SQL:

           CREATE TABLE SOE.UNITTEST (TEST TIMESTAMP WITH TIME ZONE);
           INSERT INTO SOE.UNITTEST VALUES (TO_TIMESTAMP_TZ('2016/09/01 00:00:01 +12:00', 'YYYY/MM/DD HH24:MI:SS TZH:TZM'));
           COMMIT;
         */
        final int []TIMESTAMP_WITH_TIME_ZONE = {
            13, 520647800, 131341, -805306368, 4
        };
        
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("Pacific/Auckland"));
        DateFormat dateFormatUTC = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss");
        dateFormatUTC.setTimeZone(TimeZone.getTimeZone("UTC"));
        Timestamp EXPECTED = null;
        
        try {
            Date dtz = dateFormat.parse("2016/09/01 00:00:01");
            Date d = dateFormatUTC.parse(dateFormatUTC.format(dtz));
            
            EXPECTED = new Timestamp (d.getTime());
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
        
        try {
            Timestamp decoded = 
                ColumnDataDecoder.decodeTimestampWithTz(TIMESTAMP_WITH_TIME_ZONE);
            
            logger.info ("Decoded Timestamp with time zone: " + decoded);

            assertTrue (
                "Expecting Timestamp with time zone: " + EXPECTED + ", got: " +
                decoded,
                decoded.equals(EXPECTED)
            );
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testDecodeIntervalDayToSec() {
        /* UNITTEST SQL:

           CREATE TABLE SOE.UNITTEST (TEST INTERVAL DAY(3) TO SECOND (2));
           INSERT INTO SOE.UNITTEST VALUES (INTERVAL '123 2:25:45.12' DAY(3) TO SECOND(2));
           COMMIT;
         */
        final int []INTERVAL_DAY_TO_SECOND = {
            11, 2063597696, -2023140034, 3623 
        };
        final String EXPECTED = "+123 2:25:45.120000000";
        
        try {
            String decoded =
                ColumnDataDecoder.decodeIntervalDayToSec(
                    INTERVAL_DAY_TO_SECOND
                );
            
            logger.info ("Decoded Interval Day to Second: " + decoded);
            
            assertTrue (
                "Expecting Interval Day to Second: " + decoded + ", got: " +
                decoded,
                decoded.equals (EXPECTED)
            );
        }
        catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testDecodeIntervalYearToMonth() {
        /* UNITTEST SQL:
        
           CREATE TABLE SOE.UNITTEST (TEST INTERVAL YEAR(3) TO MONTH);
           INSERT INTO SOE.UNITTEST VALUES (INTERVAL '1-3' YEAR TO MONTH);
           COMMIT;
         */
        final int []INTERVAL_YEAR_TO_MONTH = { 
            5, 16777344, 63
        };
        final String EXPECTED = "+1-3";

        try {
            String decoded =
                ColumnDataDecoder.decodeIntervalYearToMonth(
                    INTERVAL_YEAR_TO_MONTH
                );
            
            logger.info ("Decoded Interval Year to Month: " + decoded);
            
            assertTrue (
                "Expecting Interval Year to Month: " + decoded + ", got: " +
                decoded,
                decoded.equals (EXPECTED)
            );
        }
        catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }

}
