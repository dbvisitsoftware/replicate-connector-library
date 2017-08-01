package com.dbvisit.replicate.plog.file;

import static org.junit.Assert.*;

import java.io.EOFException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.plog.domain.DomainRecord;
import com.dbvisit.replicate.plog.domain.ChangeRowRecord;
import com.dbvisit.replicate.plog.domain.parser.DomainParser;
import com.dbvisit.replicate.plog.domain.parser.ChangeRowParser;
import com.dbvisit.replicate.plog.format.EntryType;
import com.dbvisit.replicate.plog.reader.DomainReader;
import com.dbvisit.replicate.plog.reader.PlogStreamReader;

/** 
 * Test PlogFile
 * 
 * Test that we can read different types of PLOGs properly, only
 * care about successfully parsing and reading each as a whole
 */
public class PlogFileTest {
    private static final Logger logger = LoggerFactory.getLogger(
        PlogFileTest.class
    );
    
    /* mimic old behavior */
    @SuppressWarnings("serial")
    private final Map<EntryType, DomainParser[]> domainParsers = 
        new HashMap<EntryType, DomainParser[]> () {{
            put (
                EntryType.ETYPE_CONTROL, 
                new DomainParser[] { 
                    new ChangeRowParser() 
                }
            );
            put (
                EntryType.ETYPE_METADATA,
                new DomainParser[] {  
                    new ChangeRowParser() 
                }
            );
            put (
                EntryType.ETYPE_LCR_DATA,
                new DomainParser[] {
                    new ChangeRowParser()
                }
            );
            put (
                EntryType.ETYPE_TRANSACTIONS, 
                new DomainParser[] { 
                    new ChangeRowParser() 
                }
            );
    }};
    
    private final int DEFAULT_FLUSH_SIZE = 1000;
    
    private final int PLOG_WITH_LCR_ID = 22;
    private final int PLOG_WITH_LCR_TIMESTAMP = 1468812549;
    private final String PLOG_WITH_LCR_FILENAME = "22.plog.1468812549";
    
    private final int PLOG_WITH_LCR_IDX = 2;
    
    private final int[] PLOG_IDS = new int[] { 
        20, 21, 22, 23
    };
    private final int[] PLOG_RECORD_COUNTS = new int[] {
        238, 195, 204, 195      
    };
    private final String[] PLOG_FNAMES = new String[] {
        "20.plog.1468812543",
        "21.plog.1468812547",
        "22.plog.1468812549",
        "23.plog.1468812553"
    };
    
    private String plogDir;

    public void init() {
        URL resURL = this.getClass().getResource("/data/mine/plog_multi_set");
        
        if (resURL == null) {
            fail ("Mine test path resource is not setup correctly");
        }
        
        plogDir = resURL.getFile().toString();
    }

    @Test
    public void testPlogOpenClose () {
        init();
        
        String fileName = plogDir + "/" + PLOG_WITH_LCR_FILENAME;
        try { 
            PlogFile plog = new PlogFile (
                PLOG_WITH_LCR_ID, 
                PLOG_WITH_LCR_TIMESTAMP,
                fileName,
                null
            );
        
            logger.info ("Opening PLOG: " + fileName);
            plog.open();
            assertTrue (plog.canUse());
            
            logger.info ("Closing PLOG: " + fileName);
            boolean closed = plog.close();
            assertTrue (closed);
        }
        catch (Exception e) {
            e.printStackTrace();
            logger.error (e.getMessage());
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testPlogRead () {
        init();
        
        readPlog (PLOG_WITH_LCR_IDX);
    }
    
    
    public void readPlog (int idx) {
        int id = PLOG_IDS[idx];
        String fname = PLOG_FNAMES[idx];
        int entryCount = PLOG_RECORD_COUNTS[idx];
        
        readPlog (id, fname, entryCount);
    }
    
    public void readPlog (int id, String fname, int entryCount) {
        readPlog (id, fname, entryCount, false, true, DEFAULT_FLUSH_SIZE);
    }
    
    public void readPlog (
        int id, 
        String fname, 
        int entryCount, 
        boolean merge,
        boolean quiet,
        int flushSize
    ) {
        String fileName = plogDir + "/" + fname;
        int timestamp = Integer.parseInt ((fileName.split ("\\."))[2]);
        PlogFile plog = null;
        
        try { 
            DomainReader domainReader = DomainReader.builder()
                .domainParsers(domainParsers)
                .mergeMultiPartRecords(merge)
                .build();
            
            plog = new PlogFile (id, timestamp, fileName, domainReader);
            logger.info ("Opening PLOG: " + fileName);
            plog.open();
            assertTrue (plog.canUse());
            
            logger.info ("Reading PLOG: " + fileName);
            PlogStreamReader reader = plog.getReader();
            
            boolean ready = reader.isReady();
            assertTrue (ready);
            
            reader.setFlushSize (flushSize);
            
            /* check for partial PLOG for testing */
            boolean partial = false;
            long offset = reader.getOffset();
            
            while (!reader.isDone() && !partial) {
                reader.read();
                
                if (offset == reader.getOffset()) {
                    logger.info (
                        "Aborting partial read of PLOG: " + fileName
                    );

                    partial = true;
                    break;
                }
                offset = reader.getOffset();
               
                if (reader.canFlush()) {
                    List<DomainRecord> drs = reader.flush();
                
                    if (!quiet) {
                        logger.info (
                            "Dumping batch of size: " + drs.size() +
                            " offset: " + reader.getOffset()
                        );
                        for (DomainRecord dr : drs) {
                            ChangeRowRecord lcr = (ChangeRowRecord)dr;
                            logger.info (lcr.toJSONString());
                        }
                    }
                    
                    drs.clear();
                    drs = null;
                }
            }
            
            logger.info ("Read " + reader.getRecordCount() + " entries");
            logger.info ("Closing PLOG: " + fileName);
            
            if (entryCount != -1) {
                assertEquals (
                    "Expecting " + entryCount + " entry records",
                    entryCount,
                    reader.getRecordCount()
                );
            }
            
            boolean closed = plog.close();
            assertTrue (closed);
        }
        catch (Exception e) {
            e.printStackTrace();
            logger.error (e.getMessage());
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testPlogReadSet () {
        init();
       
        for (int i = 0; i < PLOG_IDS.length; i++) {
            readPlog (i);
        }
    }
    
    @Test
    public void testPlogWithBlobAdd () {
        URL resURL = this.getClass().getResource(
            "/data/mine/plog_single_lobwrite"
        );
        
        if (resURL == null) {
            fail ("Mine test path resource is not setup correctly");
        }
        
        plogDir = resURL.getFile().toString();
        
        int id = 27;
        String fname = "27.plog.1467241709";
        
        readPlog (id, fname, 464);
    }
    
    @Test
    public void testPlogWithBlobUpdate () {
        URL resURL = this.getClass().getResource(
            "/data/mine/plog_single_lobupdate"
        );
            
        if (resURL == null) {
            fail ("Mine test path resource is not setup correctly");
        }
            
        plogDir = resURL.getFile().toString();
            
        int id = 24;
        String fname = "24.plog.1467163548";
            
        readPlog (id, fname, 512);
    }
    
    @Test
    public void testPlogWithBlobDelete () {
        URL resURL = this.getClass().getResource(
            "/data/mine/plog_single_lobdelete"
        );
            
        if (resURL == null) {
            fail ("Mine test path resource is not setup correctly");
        }
            
        plogDir = resURL.getFile().toString();
            
        int id = 17;
        String fname = "17.plog.1467155646";
            
        readPlog (id, fname, 375);
    }
    
    @Test
    public void testPlogWithColumnAdded () {
        URL resURL = this.getClass().getResource(
            "/data/mine/plog_single_addcolumn"
        );
            
        if (resURL == null) {
            fail ("Mine test path resource is not setup correctly");
        }
            
        plogDir = resURL.getFile().toString();
            
        int id = 12;
        String fname = "12.plog.1467080596";
        
        readPlog (id, fname, 2376);
    }
    
    @Test 
    public void testPlogWithMultiPartUpdate () {
        URL resURL = this.getClass().getResource(
            "/data/mine/plog_single_lobupdate"
        );
                
        if (resURL == null) {
            fail ("Mine test path resource is not setup correctly");
        }

        plogDir = resURL.getFile().toString();

        int id = 24;
        String fname = "24.plog.1467163548";

        readPlog (id, fname, -1, true, true, 1);
    }
    
    @Test
    public void testPlogReadInvalid () {
        URL resURL = this.getClass().getResource(
            "/data/mine/plog_invalid"
        );
                    
        if (resURL == null) {
            fail ("Mine test path resource is not setup correctly");
        }

        plogDir = resURL.getFile().toString();
        
        int id = 11;
        int timestamp = 1464925032;
        String fname = "11.plog.1464925032";
        
        String fileName = plogDir + "/" + fname;
        try {
            PlogFile plog = new PlogFile (id, timestamp, fileName, null);

            logger.info ("Opening PLOG: " + fileName);
            plog.open();
            fail ("Should have failed to open invalid PLOG: " + fileName);
        }
        catch (Exception e) {
            assertTrue (
                "Got unexpected error: " + e.getMessage(),
                e.getMessage().contains("Invalid PLOG file, unable to parse")
            );
        }
            
    }

    @Test
    public void testPlogReadEmpty () {
        URL resURL = this.getClass().getResource(
            "/data/mine/plog_invalid"
        );
                    
        if (resURL == null) {
            fail ("Mine test path resource is not setup correctly");
        }

        plogDir = resURL.getFile().toString();
        
        int id = 1;
        int timestamp = 1111111111;
        String fname = "1.plog.1111111111";
        
        String fileName = plogDir + "/" + fname;
        try {
            PlogFile plog = new PlogFile (id, timestamp, fileName, null);

            logger.info ("Opening PLOG: " + fileName);
            plog.open();
            fail ("Should have failed to open empty PLOG: " + fileName);
        }
        catch (Exception e) {
            /* empty file will have caused EOF exception */
            assertTrue (e.getCause() instanceof EOFException);
            
            assertTrue (
                e.getMessage().startsWith (
                    "Failed to open stream reader for PLOG"
                )
            );    
        }
    }

}
