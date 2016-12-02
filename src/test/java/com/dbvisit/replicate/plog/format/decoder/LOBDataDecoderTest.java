package com.dbvisit.replicate.plog.format.decoder;

import static org.junit.Assert.*;

import javax.sql.rowset.serial.SerialBlob;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Simple tests for validating decoding of the raw LOB payload of
 *  entry tag records in PLOG 
 */
public class LOBDataDecoderTest {
    private static final Logger logger = LoggerFactory.getLogger(
        LOBDataDecoderTest.class
    );
    
    @Test
    public void testDecodeCLOB() {
        /* UNITTEST SQL:
        
           CREATE TABLE SOE.UNITTEST (TEST CLOB);
           INSERT INTO SOE.UNITTEST VALUES (TO_CLOB ('<html><body><p>This is a HTML paragraph</p></body></html>'));
           COMMIT;
         */
        final int []CLOB = { 
            57, 0, 1836345404, 1648115308, 1048142959, 1413378108, 
            544434536, 1629516649, 1297369120, 1634738252, 1919377778, 
            1013477473, 1010724911, 1685021231, 792477305, 1819112552, 
            62 
        };
        final String EXPECTED = 
            "<html><body><p>This is a HTML paragraph</p></body></html>";
        /* test decoding with and without LOB length */
        final int[] LOBLEN = {
            EXPECTED.length(),
            0
        };
        
        try {
            for (int i = 0; i < LOBLEN.length; i++) {
                String decoded = LOBDataDecoder.decodeCLOB(
                    CLOB, 
                    LOBLEN[i]
                );
                logger.info (
                    "Decoded CLOB: " + decoded + ", providing LOB length: " + 
                    LOBLEN[i]
                );
            
                assertTrue (
                    "Expecting CLOB: " + EXPECTED + ", got: " + decoded,
                    decoded.equals(EXPECTED)
                );
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testDecodeNCLOB() {
        /* UNITTEST SQL:
        
           CREATE TABLE SOE.UNITTEST (TEST NCLOB);
           INSERT INTO SOE.UNITTEST VALUES (TO_NCLOB ('<html><body><p>This is a HTML paragraph</p></body></html>'));
           COMMIT;
         */
        final int []NCLOB = { 
            57, 0, 1836345404, 1648115308, 1048142959, 1413378108,
            544434536, 1629516649, 1297369120, 1634738252, 1919377778,
            1013477473, 1010724911, 1685021231, 792477305, 1819112552,
            62
        };
        final String EXPECTED = 
            "<html><body><p>This is a HTML paragraph</p></body></html>";
        /* test decoding with and without LOB length */
        final int[] LOBLEN = {
            EXPECTED.length(),
            0
        };
        
        try {
            for (int i = 0; i < LOBLEN.length; i++) {
                String decoded = LOBDataDecoder.decodeCLOB(
                    NCLOB, 
                    LOBLEN[i]
                );
                logger.info (
                    "Decoded NCLOB: " + decoded + ", providing LOB length: " + 
                    LOBLEN[i]
                );
            
                assertTrue (
                    "Expecting NCLOB: " + EXPECTED + ", got: " + decoded,
                    decoded.equals(EXPECTED)
                );
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testDecodeBLOB() {
        /* UNITTEST SQL:
        
           CREATE TABLE SOE.UNITTEST (TEST BLOB);
           INSERT INTO SOE.UNITTEST VALUES (TO_BLOB('FF3311121212EE3A'));
           COMMIT;
         */
        final int []BLOB = { 
            8, 0, 303117311, 988680722
        };
        
        final String EXPECTED = 
            "FF3311121212EE3A";
        /* test decoding with and without LOB length */
        final int[] LOBLEN = {
            EXPECTED.length() >> 1,
            0
        };

        try {
            for (int i = 0; i < LOBLEN.length; i++) {
                SerialBlob sb = LOBDataDecoder.decodeBLOB(
                    BLOB,
                    LOBLEN[i]
                );
                
                String decoded = 
                    DataDecoder.bytesToHex(
                        sb.getBytes(1, (int)sb.length())
                    );
                
                logger.info (
                    "Decoded BLOB: " + decoded + ", providing LOB length: " + 
                    LOBLEN[i]
                );

                assertTrue (
                    "Expecting BLOB: " + EXPECTED + ", got: " + decoded,
                    decoded.equals(EXPECTED)
                );
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }

}
