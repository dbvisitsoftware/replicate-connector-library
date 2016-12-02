package com.dbvisit.replicate.plog.file;

import static org.junit.Assert.*;

import java.net.URL;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.plog.config.PlogConfig;
import com.dbvisit.replicate.plog.config.PlogConfigType;
import com.dbvisit.replicate.plog.file.PlogFileManager;

/** 
 * Test the PlogFileManager
 */
public class PlogFileManagerTest {
    private static final Logger logger = LoggerFactory.getLogger(
        PlogFileManagerTest.class
    );
    
    private final int OLDEST_PLOG_ID = 20;
    private final String OLDEST_PLOG_FILE = "20.plog.1468812543";
    private final int[] PLOG_SET_IDS = { 20, 21, 22, 23 };
    
    private final String[] PLOG_SET_FILENAMES = {
        "492.plog.1473711131",
        "493.plog.1473711144",
        "494.plog.1473711144",
        "495.plog.1473711144",
        "496.plog.1473711145"
    };
    
    private final String[] PLOG_MINE_RESTART_SET_FILENAMES = {
        "937.plog.1478165129",
        "937.plog.1478197765",
        "938.plog.1478165179",
        "938.plog.1478197766"
    };
    
    public PlogConfig getConfigForMultiSet () throws Exception {
        PlogConfig config = new PlogConfig();
        
        URL resURL = this.getClass().getResource("/data/mine/plog_multi_set");
            
        if (resURL == null) {
            fail ("Mine test path resource is not setup correctly");
        }

        config.setConfigValue(
            PlogConfigType.PLOG_LOCATION_URI, 
            resURL.toString()
        );
        
        config.setConfigValue (
            PlogConfigType.SCAN_QUIT_INTERVAL_COUNT,
            "1"
        );
        
        return config;
    }
    
    public PlogConfig getConfigForSingle () throws Exception {
        PlogConfig config = new PlogConfig();
        
        URL resURL = this.getClass().getResource("/data/mine/plog_single");
            
        if (resURL == null) {
            fail ("Mine test path resource is not setup correctly");
        }

        config.setConfigValue (
            PlogConfigType.PLOG_LOCATION_URI, 
            resURL.toString()
        );
        
        config.setConfigValue (
            PlogConfigType.SCAN_QUIT_INTERVAL_COUNT,
            "1"
        );
        
        return config;
    }
    
    public PlogConfig getConfigForLoadSet () throws Exception {
        PlogConfig config = new PlogConfig();
        
        URL resURL = this.getClass().getResource("/data/mine/plog_load_set");
            
        if (resURL == null) {
            fail ("Mine test path resource is not setup correctly");
        }

        config.setConfigValue(
            PlogConfigType.PLOG_LOCATION_URI, 
            resURL.toString()
        );
        
        config.setConfigValue (
            PlogConfigType.SCAN_QUIT_INTERVAL_COUNT,
            "1"
        );
        
        return config;
    }
    
    public PlogConfig getConfigForMultiPartSet () throws Exception {
        PlogConfig config = new PlogConfig();
        
        URL resURL = this.getClass()
                         .getResource("/data/mine/plog_multi_part_set");
            
        if (resURL == null) {
            fail ("Mine test path resource is not setup correctly");
        }

        config.setConfigValue(
            PlogConfigType.PLOG_LOCATION_URI, 
            resURL.toString()
        );
        
        config.setConfigValue (
            PlogConfigType.SCAN_QUIT_INTERVAL_COUNT,
            "1"
        );
        
        return config;
    }
    
    @Test
    public void testLatestPlogFile () {
        try {
            PlogFileManager fm = new PlogFileManager (getConfigForMultiSet());
            
            int oldest = fm.findOldestPlogSequence();
            
            logger.info (
                "Found oldest PLOG sequence: " + OLDEST_PLOG_ID
            );
            assertEquals (
                "Oldest PLOG sequence must be " + OLDEST_PLOG_ID, 
                oldest, 
                OLDEST_PLOG_ID
            );
            
            fm.findNextPlogFile(OLDEST_PLOG_ID);
            String filename = fm.getPlog().getFileName();
            
            filename = filename.substring (filename.lastIndexOf("/") + 1);
            
            logger.info (
                "Found PLOG file for sequence: " + OLDEST_PLOG_ID + 
                " file: " + filename
            );
            
            assertEquals (
                "PLOG file for sequence: " + OLDEST_PLOG_ID + " must be " +
                OLDEST_PLOG_FILE,
                filename,
                OLDEST_PLOG_FILE
            );
        }
        catch (Exception e) {
            logger.error (e.getMessage());
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testScanSinglePlogFile () {
        try {
            PlogFileManager fm = new PlogFileManager (getConfigForSingle());
            
            fm.scan();
            PlogFile plog = fm.getPlog();
            
            fm.setForceInterrupt();
                    
            while (plog != null) {
                logger.info ("Found " + plog);
                
                fm.scan();
                plog = fm.getPlog();
            }
        }
        catch (InterruptedException ie) {
            logger.info ("Forcefully interrupted");
        }
        catch (Exception e) {
            logger.error (e.getMessage());
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testScanPlogFiles () {
        int i = 0;
        try {
            PlogFileManager fm = new PlogFileManager (getConfigForMultiSet());
            
            fm.scan();
            PlogFile plog = fm.getPlog();
            
            fm.setForceInterrupt();
            
            while (plog != null) {
                logger.info ("Found " + plog);

                assertTrue (plog.isValid());
                assertTrue (plog.canUse());
                assertFalse (!plog.getReader().isBusy());
                assertFalse (plog.getReader().isDone());
                assertEquals (
                    "Current PLOG ID must be " + PLOG_SET_IDS[i],
                    plog.getId(),
                    PLOG_SET_IDS[i]
                );
                
                i++;
                fm.scan();
                plog = fm.getPlog();
            }
            
        }
        catch (InterruptedException ie) {
            logger.info ("Forcefully interrupted");
            assertEquals (
                "Must have found " + PLOG_SET_IDS.length + " PLOGS",
                PLOG_SET_IDS.length,
                i
            );
        }
        catch (Exception e) {
            e.printStackTrace();
            logger.error (e.getMessage());
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testScanWithLoadPlogFiles () {
        int i = 0;
        try {
            PlogFileManager fm = new PlogFileManager (getConfigForLoadSet());
            
            fm.scan();
            PlogFile plog = fm.getPlog();
            
            fm.setForceInterrupt();
            
            while (plog != null) {
                logger.info ("Found " + plog);

                assertTrue (plog.isValid());
                assertTrue (plog.canUse());
                assertFalse (!plog.getReader().isBusy());
                assertFalse (plog.getReader().isDone());
                
                String fileName = plog.getFileName().substring(
                    plog.getFileName().lastIndexOf('/') + 1
                );
                
                assertTrue (
                    "Expecting LOAD file: " + PLOG_SET_FILENAMES[i] +
                    ", got: " + fileName,
                    fileName.equals(PLOG_SET_FILENAMES[i])
                );
                
                i++;
                fm.scan();
                plog = fm.getPlog();
            }
            
        }
        catch (InterruptedException ie) {
            logger.info ("Forcefully interrupted");
        }
        catch (Exception e) {
            e.printStackTrace();
            logger.error (e.getMessage());
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testScanWithMultiPartPlogFiles () {
        int i = 0;
        try {
            PlogFileManager fm = new PlogFileManager (
                getConfigForMultiPartSet()
            );
            
            fm.scan();
            PlogFile plog = fm.getPlog();
            
            fm.setForceInterrupt();
            
            while (plog != null) {
                logger.info ("Found " + plog);

                assertTrue (plog.isValid());
                assertTrue (plog.canUse());
                assertFalse (!plog.getReader().isBusy());
                assertFalse (plog.getReader().isDone());
                
                String fileName = plog.getFileName().substring(
                    plog.getFileName().lastIndexOf('/') + 1
                );
                
                assertTrue (
                    "Expecting multi-part PLOG file: " + 
                    PLOG_MINE_RESTART_SET_FILENAMES[i] + ", got: "  + 
                    fileName,
                    fileName.equals(PLOG_MINE_RESTART_SET_FILENAMES[i])
                );
                
                i++;
                fm.scan();
                plog = fm.getPlog();
            }
            
        }
        catch (InterruptedException ie) {
            logger.info ("Forcefully interrupted");
        }
        catch (Exception e) {
            e.printStackTrace();
            logger.error (e.getMessage());
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testStartScanAtMultiPartPlogFile () {
        int i = 0;
        try {
            /* start at second PLOG for first restart */
            i = 2;
            String[] parts = PLOG_MINE_RESTART_SET_FILENAMES[i].split("\\.");
            
            PlogFileManager fm = new PlogFileManager (
                getConfigForMultiPartSet(),
                PlogFile.createPlogUID(
                    Integer.parseInt (parts[0]),
                    Integer.parseInt (parts[2])
                )
            );
            
            fm.scan();
            PlogFile plog = fm.getPlog();
            
            fm.setForceInterrupt();
            
            while (plog != null) {
                logger.info ("Found " + plog);

                assertTrue (plog.isValid());
                assertTrue (plog.canUse());
                assertFalse (!plog.getReader().isBusy());
                assertFalse (plog.getReader().isDone());
                
                String fileName = plog.getFileName().substring(
                    plog.getFileName().lastIndexOf('/') + 1
                );
                
                assertTrue (
                    "Expecting multi-part PLOG file: " + 
                    PLOG_MINE_RESTART_SET_FILENAMES[i] + ", got: "  + 
                    fileName,
                    fileName.equals(PLOG_MINE_RESTART_SET_FILENAMES[i])
                );
                
                i++;
                fm.scan();
                plog = fm.getPlog();
            }
            
        }
        catch (InterruptedException ie) {
            logger.info ("Forcefully interrupted");
        }
        catch (Exception e) {
            e.printStackTrace();
            logger.error (e.getMessage());
            fail (e.getMessage());
        }
    }
    
}
