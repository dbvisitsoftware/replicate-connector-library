package com.dbvisit.replicate.plog.reader;

import static org.junit.Assert.*;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.rowset.serial.SerialBlob;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.plog.domain.ChangeRowRecord;
import com.dbvisit.replicate.plog.domain.TransactionInfoRecord;
import com.dbvisit.replicate.plog.domain.parser.DomainParser;
import com.dbvisit.replicate.plog.domain.parser.ChangeRowParser;
import com.dbvisit.replicate.plog.domain.parser.MetaDataParser;
import com.dbvisit.replicate.plog.domain.parser.TransactionInfoParser;
import com.dbvisit.replicate.plog.domain.ChangeAction;
import com.dbvisit.replicate.plog.domain.ColumnValue;
import com.dbvisit.replicate.plog.domain.DomainRecord;
import com.dbvisit.replicate.plog.file.PlogFile;
import com.dbvisit.replicate.plog.format.EntrySubType;
import com.dbvisit.replicate.plog.format.EntryType;
import com.dbvisit.replicate.plog.reader.criteria.TypeCriteria;

/**
 * Tests reading of LCR data from PLOGs. This includes parsing and decoding
 * raw data for INSERTS/UPDATES/DELETEs with and without LOB data, eg.
 * multi-part records.
 */
public class PlogReaderTest {
    private static final Logger logger = LoggerFactory.getLogger(
        PlogReaderTest.class
    );
        
    public final Object[] INSERT_SQL = new Object[] {
        2, 
        "Second", 
        "Customer", 
        "en", 
        "nz", 
        "99.99", 
        "first.customer@customers.com", 
        1
    };
    
    public final Object[] UPDATE_SQL_2 = new Object[] {
        1, 
        "First", 
        "Customer", 
        "en", 
        "nz", 
        "99.00", 
        "first.customer@customers.com", 
        1
    };
    
    public final Object[] UPDATE_SQL_1 = new Object[] {
        2, 
        "Second", 
        "Customer", 
        "en", 
        "nz", 
        "99.00", 
        "first.customer@customers.com", 
        1
    };
    
    public final Object[] UPDATE_SQL_3 = new Object[] {
        3, 
        "Third", 
        "Customer", 
        "en", 
        "nz", 
        "99.00", 
        "first.customer@customers.com", 
        1
    };
    
    public final Object[] UPDATE_SQL = new Object[] {
        UPDATE_SQL_1,
        UPDATE_SQL_2,
        UPDATE_SQL_3
    };
    
    public final Object[] DELETE_SQL_1 = UPDATE_SQL_1;
    public final Object[] DELETE_SQL_2 = UPDATE_SQL_2;
    public final Object[] DELETE_SQL_3 = UPDATE_SQL_3;
    
    public final Object[] DELETE_SQL = new Object[] {
        DELETE_SQL_1,
        DELETE_SQL_2,
        DELETE_SQL_3
    };
    
    /* 
     * WAREHOUSE_ID   NOT NULL NUMBER(6)
     * WAREHOUSE_NAME          VARCHAR2(35)
     * LOCATION_ID             NUMBER(4)
     * PHOTO                   BLOB
     */
    
    String [] INSERT_ACTIONS = new String [] {
        "INSERT",
        "LOB WRITE",
        "INSERT"
    };
    
    public final Object[] INSERT_WITH_LOB_SQL_1 = new Object[] {
        99,
        "Test 99",
        1,
        null
    };

    public final Object[] INSERT_WITH_LOB_SQL_2 = new Object[] {
        100,
        "Test Insert BLOB",
        0,
        getBlobBytes ("FF3311121212EE3A")
    };
    
    public final Object[] INSERT_WITH_LOB_SQL_3 = new Object[] {
        100,
        "Test Insert BLOB",
        0,
        null
    };
    
    public final Object[] INSERT_WITH_LOB_SQL = new Object[] {
        INSERT_WITH_LOB_SQL_1,
        INSERT_WITH_LOB_SQL_2,
        INSERT_WITH_LOB_SQL_3
    };
    
    String [] UPDATE_ACTIONS = new String [] {
        "UPDATE",
        "LOB WRITE",
        "UPDATE"
    };
        
    public final Object[] UPDATE_WITH_LOB_SQL_1 = new Object[] {
        100,
        "Test 100 and 1",
        0,
        null
    };

    /* LOB WRITE only key value and blob field */
    public final Object[] UPDATE_WITH_LOB_SQL_2 = new Object[] {
        100,
        null,
        null,
        getBlobBytes ("0321")
    };

    public final Object[] UPDATE_WITH_LOB_SQL_3 = new Object[] {
        100,
        "Test 100 and 1",
        0,
        null
    };

    public final Object[] UPDATE_WITH_LOB_SQL = new Object[] {
        UPDATE_WITH_LOB_SQL_1,
        UPDATE_WITH_LOB_SQL_2,
        UPDATE_WITH_LOB_SQL_3
    };
        
    /* for delete LOB field is NULL */
    public final Object[] DELETE_WITH_LOB_SQL = new Object[] {
        100,
        "Test Insert BLOB",
        0,
        null
    };
    
    public final Object[] MERGED_INSERT_WITH_LOB_SQL_1 = INSERT_WITH_LOB_SQL_1;
    
    public final Object[] MERGED_WITH_LOB_SQL_2 = new Object[] {
        100,
        "Test Insert BLOB",
        0,
        getBlobBytes ("FF3311121212EE3A")
    };
    
    public final Object[] MERGED_INSERT_WITH_LOB_SQL = new Object[] {
        MERGED_INSERT_WITH_LOB_SQL_1,
        MERGED_WITH_LOB_SQL_2
    };
    
    public final Object[] MERGED_UPDATE_WITH_LOB_SQL_1 = new Object[] {
        100,
        "Test 100 and 1",
        0,
        null
    };

    public final Object[] MERGED_UPDATE_WITH_LOB_SQL_2 = new Object[] {
        100,
        "Test 100 and 1",
        0,
        getBlobBytes ("0321")
    };
    
    public final Object[] MERGED_UPDATE_WITH_LOB_SQL= new Object[] {
        MERGED_UPDATE_WITH_LOB_SQL_1,
        MERGED_UPDATE_WITH_LOB_SQL_2
    };

    private byte[] getBlobBytes (String val) {
        return val.getBytes();
    }
    
    @Test
    public void testSingleInsert () {
        final int PLOG_WITH_LCR_INSERT_ID = 19;
        final int PLOG_WITH_LCR_INSERT_TIMESTAMP = 1469588821;
        final String PLOG_WITH_LCR_INSERT_FILENAME = "19.plog.1469588821";
        
        final String PLOG_LOCATION = "/data/mine/plog_single_insert";
        
        URL resURL = this.getClass().getResource(PLOG_LOCATION);
        
        if (resURL == null) {
            fail ("Mine test path resource is not setup correctly");
        }
        
        String plogDir = resURL.getFile().toString();
        
        String fileName = plogDir + "/" + PLOG_WITH_LCR_INSERT_FILENAME;
        PlogFile plog = null;
        try {
            plog = new PlogFile (
                PLOG_WITH_LCR_INSERT_ID,
                PLOG_WITH_LCR_INSERT_TIMESTAMP,
                fileName,
                DomainReader.builder()
                    .persistCriteria(persistCriteria)
                    .domainParsers(domainParsers)
                    .build()
            );
            
            plog.open ();
            
            PlogStreamReader reader = plog.getReader();
            
            /* for testing make flush size 100, but PLOG should contain only 1
             * LCR data record
             */
            reader.setFlushSize(100);
            
            /* only for single data record */
            while (!reader.isDone()) {
                reader.read();
            }
            
            List <DomainRecord> drs = reader.flush();
            
            logger.info (
                "Parsed and persisted: " + drs.size() + " domain records"
            );
            
            /* first record is LCR */
            ChangeRowRecord lcr = (ChangeRowRecord)drs.get(0);
            /* second is TX for LCR */
            TransactionInfoRecord txr = (TransactionInfoRecord)drs.get(1);
            
            assertTrue (
                "Expecting 1 INSERT LCR and 1 TX", 
                drs.size() == 2 && 
                lcr.getAction().equals (ChangeAction.INSERT) &&
                txr.getRecordCount() == 1
            );
            
            logger.info (lcr.toJSONString());
            
            for (int c = 0; c < lcr.getColumnValues().size(); c++) {
                ColumnValue cr = lcr.getColumnValues().get(c);
                
                logger.info (cr.toString());
                assertTrue (
                    cr.getValue() + " must be same as " + INSERT_SQL[c],
                    cr.getValue().toString().equals (INSERT_SQL[c].toString())
                );
            }
            
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
        finally {
            if (plog != null) {
                plog.close();
            }
        }
    }
    
    @Test
    public void testUpdate () {
        final int PLOG_WITH_LCR_UPDATE_ID = 21;
        final String PLOG_WITH_LCR_UPDATE_FILENAME = "21.plog.1469589181";
        final int PLOG_WITH_LCR_UPDATE_TIMESTAMP = 1469589181;
        final String PLOG_LOCATION = "/data/mine/plog_single_update";
        
        URL resURL = this.getClass().getResource(PLOG_LOCATION);
            
        if (resURL == null) {
            fail ("Mine test path resource is not setup correctly");
        }

        String plogDir = resURL.getFile().toString();

        String fileName = plogDir + "/" + PLOG_WITH_LCR_UPDATE_FILENAME;
        PlogFile plog = null;

        try {
            plog = new PlogFile (
                PLOG_WITH_LCR_UPDATE_ID,
                PLOG_WITH_LCR_UPDATE_TIMESTAMP,
                fileName,
                DomainReader.builder()
                    .persistCriteria(persistCriteria)
                    .domainParsers(domainParsers)
                    .build()
            );
            
            plog.open ();

            PlogStreamReader reader = plog.getReader();
            
            /* for testing make flush size 100, but PLOG should contain only 1
             * LCR data record
             */
            reader.setFlushSize(100);

            while (!reader.isDone()) {
                reader.read();
            }
            
            List <DomainRecord> drs = reader.flush();
            
            assertTrue (
                "Expecting 4 UPDATE LCR and 1 TX, but found " + drs.size(), 
                drs.size() == 4
            );

            int l = 0;
            for (DomainRecord dr : drs) {
                logger.info (dr.toJSONString());
                
                if (dr.isChangeRowRecord()) {
                    ChangeRowRecord lcr = (ChangeRowRecord)dr;
                    
                    logger.info (lcr.toJSONString());
                    
                    assertTrue (
                        "Expect UPDATE LCR, but found: " + 
                        lcr.getAction().toString(),
                        lcr.getAction().equals (ChangeAction.UPDATE)
                    );

                    for (int c = 0; c < lcr.getColumnValues().size(); c++) {
                        ColumnValue cr = lcr.getColumnValues().get(c);
                        
                        logger.info (cr.toString());
                        
                        Object[] update = (Object[])UPDATE_SQL[l];
                        assertTrue (
                            cr.getValue() + " must be same as " + update[c],
                            cr.getValue().toString().equals (update[c].toString())
                        );
                    }
                    l++;
                }
                
                if (dr.isTransactionInfoRecord()) {
                    TransactionInfoRecord txr = (TransactionInfoRecord)dr;
                
                    assertTrue (
                        "Expecting 3 records in TX for UPDATE",
                        txr.getRecordCount() == 3
                    );
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
        finally {
            if (plog != null) {
                plog.close();
            }
        }
    }
    
    @Test
    public void testDelete () {
        final int PLOG_WITH_LCR_DELETE_ID = 22;
        final int PLOG_WITH_LCR_DELETE_TIMESTAMP = 1469589193;
        final String PLOG_WITH_LCR_DELETE_FILENAME = "22.plog.1469589193";
        final String PLOG_LOCATION = "/data/mine/plog_single_delete";
        
        URL resURL = this.getClass().getResource(PLOG_LOCATION);
            
        if (resURL == null) {
            fail ("Mine test path resource is not setup correctly");
        }

        String plogDir = resURL.getFile().toString();

        String fileName = plogDir + "/" + PLOG_WITH_LCR_DELETE_FILENAME;
        PlogFile plog = null;

        try {
            plog = new PlogFile (
                PLOG_WITH_LCR_DELETE_ID, 
                PLOG_WITH_LCR_DELETE_TIMESTAMP,
                fileName,
                DomainReader.builder()
                    .persistCriteria(persistCriteria)
                    .domainParsers(domainParsers)
                    .build()
            );
            plog.open ();

            PlogStreamReader reader = plog.getReader();
            
            /* for testing make flush size 100, but PLOG should contain only 1
             * LCR data record
             */
            reader.setFlushSize(100);

            while (!reader.isDone()) {
                reader.read();
            }

            List<DomainRecord> drs = reader.flush();

            assertTrue (
                "Expecting 3 DELETE LCR and 1 TX, but found " + drs.size(), 
                drs.size() == 4
            );

            int l = 0;
            for (DomainRecord dr : drs) {
                logger.info (dr.toJSONString());
                
                if (dr.isChangeRowRecord()) {
                    ChangeRowRecord lcr = (ChangeRowRecord)dr;
                    
                    assertTrue (
                        "Expect DELETE LCR, but found: " + 
                        lcr.getAction().toString(),
                        lcr.getAction().equals (ChangeAction.DELETE)
                    );

                    Object[] delete = (Object[])DELETE_SQL[l];
                    for (int c = 0; c < lcr.getColumnValues().size(); c++) {
                        ColumnValue cr = lcr.getColumnValues().get(c);

                        logger.info (cr.toString());
                        
                        assertTrue (
                            cr.getValue() + " must be same as " + delete[c],
                            cr.getValue().toString().equals (delete[c].toString())
                        );
                    }
                    l++;
                }
                
                if (dr.isTransactionInfoRecord()) {
                    TransactionInfoRecord txr = (TransactionInfoRecord)dr;
                    
                    assertTrue (
                        "Expecting 3 records in TX for DELETE",
                        txr.getRecordCount() == 3
                    );
                }
            }
            
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
        finally {
            if (plog != null) {
                plog.close();
            }
        }
    }

    @Test
    public void testInsertRowWithBlob () {
        final int PLOG_WITH_LCR_INSERT_ID = 27;
        final int PLOG_WITH_LCR_INSERT_TIMESTAMP = 1467241709;
        final String PLOG_WITH_LCR_INSERT_FILENAME = "27.plog.1467241709";
        final String PLOG_LOCATION = "/data/mine/plog_single_lobwrite";
        
        URL resURL = this.getClass().getResource(PLOG_LOCATION);
            
        if (resURL == null) {
            fail ("Mine test path resource is not setup correctly");
        }

        String plogDir = resURL.getFile().toString();

        String fileName = plogDir + "/" + PLOG_WITH_LCR_INSERT_FILENAME;
        PlogFile plog = null;

        try {
            plog = new PlogFile (
                PLOG_WITH_LCR_INSERT_ID, 
                PLOG_WITH_LCR_INSERT_TIMESTAMP,
                fileName,
                DomainReader.builder()
                    .persistCriteria(persistCriteria)
                    .domainParsers(domainParsers)
                    .build()
            );
            
            plog.open ();

            PlogStreamReader reader = plog.getReader();
            
            /* for testing make flush size 100, but PLOG should contain only 1
             * LCR data record
             */
            reader.setFlushSize(100);

            while (!reader.isDone()) {
                reader.read();
            }

            List<DomainRecord> drs = reader.flush();
            
            assertTrue (
                "Expecting 3 LCR (row with LOB field) and 3 TX, but found " + 
                drs.size(), 
                drs.size() == 3 + 3
            );
            
            int l = 0;
            for (DomainRecord dr : drs) {
                logger.info (dr.toJSONString());
                if (dr.isChangeRowRecord()) {
                    ChangeRowRecord lcr = (ChangeRowRecord)dr;
                
                    String action = INSERT_ACTIONS[l];
                   
                    assertTrue (
                        "Expect " + action + " LCR, but found: " + 
                        lcr.getAction().toString(),
                        lcr.getAction().toString().equals (action)
                    );

                    Object[] insert = (Object[])INSERT_WITH_LOB_SQL[l];
                    for (int c = 0; c < lcr.getColumnValues().size(); c++) {
                        ColumnValue cr = lcr.getColumnValues().get(c);

                        if (cr != null && cr.getValue() != null) {
                            logger.info (cr.toString());
                        
                            if (cr.getValue() instanceof SerialBlob) {
                                String b1str = cr.getValueAsString();
                                String b2str = new String((byte[])insert[c]);
                            
                                assertTrue (
                                    "Decoded blob: " + b1str + " " +
                                    "must be same as: " + b2str,
                                    b1str.equals (b2str)
                                );
                            }
                            else {
                                assertTrue (
                                    "Decoded value: " + cr.getValue() + " " +
                                    "must be same as: " + insert[c],
                                    cr.getValue().equals (insert[c])
                                );
                            }
                        }
                        else {
                            assertTrue (
                                "Must be null",
                                insert[c] == null
                            );
                        }
                    }
                    l++;
                }
                
                if (dr.isTransactionInfoRecord()) {
                    TransactionInfoRecord txr = (TransactionInfoRecord)dr;
                    
                    assertTrue (
                        "Expecting 1 TX per LOB write",
                        txr.getRecordCount() == 1
                    );
                }
            }            
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
        finally {
            if (plog != null) {
                plog.close();
            }
        }
    }
    
    @Test
    public void testUpdateRowWithBlob () {
        final int PLOG_WITH_LCR_UPDATE_ID = 24;
        final int PLOG_WITH_LCR_UPDATE_TIMESTAMP = 1467163548;
        final String PLOG_WITH_LCR_UPDATE_FILENAME = "24.plog.1467163548";
        final String PLOG_LOCATION = "/data/mine/plog_single_lobupdate";
        
        URL resURL = this.getClass().getResource(PLOG_LOCATION);
            
        if (resURL == null) {
            fail ("Mine test path resource is not setup correctly");
        }

        String plogDir = resURL.getFile().toString();

        String fileName = plogDir + "/" + PLOG_WITH_LCR_UPDATE_FILENAME;
        PlogFile plog = null;

        try {
            plog = new PlogFile (
                PLOG_WITH_LCR_UPDATE_ID, 
                PLOG_WITH_LCR_UPDATE_TIMESTAMP,
                fileName,
                DomainReader.builder()
                    .persistCriteria(persistCriteria)
                    .domainParsers(domainParsers)
                    .build()
            );
            plog.open ();

            PlogStreamReader reader = plog.getReader();
            
            /* for testing make flush size 100, but PLOG should contain only 1
             * LCR data record
             */
            reader.setFlushSize(100);

            while (!reader.isDone()) {
                reader.read();
            }

            List<DomainRecord> drs = reader.flush();
            
            assertTrue (
                "Expecting 3 LCR (row with LOB field) and 3 TXs, but found " + 
                drs.size(), 
                drs.size() == 3 + 3
            );
            
            int l = 0;
            for (DomainRecord dr : drs) {
                if (dr.isChangeRowRecord()) {
                    ChangeRowRecord lcr = (ChangeRowRecord)dr;
                    
                    String action = UPDATE_ACTIONS[l];
                    logger.info (lcr.toJSONString());
                    
                    assertTrue (
                        "Expect " + action + " LCR, but found: " + 
                        lcr.getAction().toString(),
                        lcr.getAction().toString().equals (action)
                    );

                    Object[] update = (Object[])UPDATE_WITH_LOB_SQL[l];
                    
                    for (int c = 0; c < lcr.getColumnValues().size(); c++) {
                        ColumnValue cr = lcr.getColumnValues().get(c);

                        if (cr != null) {
                            logger.info (cr.toString());
                            
                            if (cr.getValue() instanceof SerialBlob) {
                                String b1str = cr.getValueAsString();
                                String b2str = new String((byte[])update[c]);
                                
                                assertTrue (
                                    "Decoded blob: " + b1str + " " +
                                    "must be same as: " + b2str,
                                    b1str.equals (b2str)
                                );
                            }
                            else {
                                assertTrue (
                                    "Decoded value: " + cr.getValue() + " " +
                                    "must be same as: " + update[c],
                                    cr.getValue().equals (update[c])
                                );
                            }
                        }
                        else {
                            assertTrue (
                                "Must be null",
                                cr == null && update[c] == null
                            );
                        }
                    }
                    l++;
                }
                
                if (dr.isTransactionInfoRecord()) {
                    TransactionInfoRecord txr = (TransactionInfoRecord)dr;
                    
                    assertTrue (
                        "Expecting 1 TX per LOB update",
                        txr.getRecordCount() == 1
                    );
                }
            }            
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
        finally {
            if (plog != null) {
                plog.close();
            }
        }
    }
    @Test
    public void testDeleteRowWithBlob () {
        final int PLOG_WITH_LCR_DELETE_ID = 17;
        final int PLOG_WITH_LCR_DELETE_TIMESTAMP = 1467155646;
        final String PLOG_WITH_LCR_DELETE_FILENAME = "17.plog.1467155646";
        final String PLOG_LOCATION = "/data/mine/plog_single_lobdelete";
        
        URL resURL = this.getClass().getResource(PLOG_LOCATION);
            
        if (resURL == null) {
            fail ("Mine test path resource is not setup correctly");
        }

        String plogDir = resURL.getFile().toString();

        String fileName = plogDir + "/" + PLOG_WITH_LCR_DELETE_FILENAME;
        PlogFile plog = null;

        try {
            plog = new PlogFile (
                PLOG_WITH_LCR_DELETE_ID, 
                PLOG_WITH_LCR_DELETE_TIMESTAMP,
                fileName,
                DomainReader.builder()
                    .persistCriteria(persistCriteria)
                    .domainParsers(domainParsers)
                    .build()
            );
            plog.open ();

            PlogStreamReader reader = plog.getReader();
            
            /* for testing make flush size 100, but PLOG should contain only 1
             * LCR data record
             */
            reader.setFlushSize(100);

            while (!reader.isDone()) {
                reader.read();
            }

            List<DomainRecord> drs = reader.flush();
            ChangeRowRecord lcr = (ChangeRowRecord)drs.get (0);

            assertTrue (
                "Expecting 1 DELETE LCR (with LOB) and 1 TX, but found " + 
                drs.size(), 
                drs.size() == 2 && 
                lcr.getAction().equals (ChangeAction.DELETE)
            );

            Object[] delete = (Object[])DELETE_WITH_LOB_SQL;
            
            for (int c = 0; c < lcr.getColumnValues().size(); c++) {
                ColumnValue cr = lcr.getColumnValues().get(c);
                
                if (cr != null && cr.getValue() != null) {
                    logger.info (cr.toString());

                    assertTrue (
                        cr.getValue() + " must be same as " + delete[c],
                        cr.getValue().toString().equals (delete[c].toString())
                    );
                }
                else {
                    assertTrue (
                        "Must be null",
                        delete[c] == null
                    );
                }
            }
            
            assertTrue (
                "Expecting second record to be TX",
                drs.get(1).isTransactionInfoRecord()
            );
            
            TransactionInfoRecord txr = (TransactionInfoRecord)drs.get (1);
            
            assertTrue (
                "Expecting 1 TX record containing 1 DELETE record",
                txr.getRecordCount() == 1
            );
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
        finally {
            if (plog != null) {
                plog.close();
            }
        }
    }
    
    @Test
    public void testMergeMultiPartInsert () {
        final int PLOG_WITH_LCR_INSERT_ID = 27;
        final int PLOG_WITH_LCR_INSERT_TIMESTAMP = 1467241709;
        final String PLOG_WITH_LCR_INSERT_FILENAME = "27.plog.1467241709";
        final String PLOG_LOCATION = "/data/mine/plog_single_lobwrite";
        
        URL resURL = this.getClass().getResource(PLOG_LOCATION);
            
        if (resURL == null) {
            fail ("Mine test path resource is not setup correctly");
        }

        String plogDir = resURL.getFile().toString();

        String fileName = plogDir + "/" + PLOG_WITH_LCR_INSERT_FILENAME;
        PlogFile plog = null;

        try {
            /* test merging */
            plog = new PlogFile (
                PLOG_WITH_LCR_INSERT_ID, 
                PLOG_WITH_LCR_INSERT_TIMESTAMP,
                fileName,
                DomainReader.builder()
                    .persistCriteria(persistCriteria)
                    .domainParsers(domainParsers)
                    .mergeMultiPartRecords(true)
                    .build()
            );
            plog.open ();

            PlogStreamReader reader = plog.getReader();
            
            /* for testing make flush size 100, but PLOG should contain only 1
             * LCR data record
             */
            reader.setFlushSize(100);

            while (!reader.isDone()) {
                reader.read();
            }

            List<DomainRecord> drs = reader.flush();
            
            assertTrue (
                "Expecting 2 LCR (LOB records merged with INSERT) and " +
                "2 TXs, but found " + drs.size(), 
                drs.size() == 2 + 2
            );
            
            int l = 0;
            
            for (DomainRecord dr : drs) {
                logger.info (dr.toJSONString());
                
                if (dr.isChangeRowRecord()) {
                    ChangeRowRecord lcr = (ChangeRowRecord)dr;
                    
                    assertTrue (
                        "Merged INSERTS only",
                        lcr.getAction().equals (ChangeAction.INSERT)
                    );
                    
                    Object[] merged = (Object[])MERGED_INSERT_WITH_LOB_SQL[l];
                    
                    for (int c = 0; c < lcr.getColumnValues().size(); c++) {
                        ColumnValue cr = lcr.getColumnValues().get(c);
                        
                        if (cr != null && cr.getValue() != null) {
                            logger.info (cr.toString());
                            
                            if (cr.getValue() instanceof SerialBlob) {
                                String b1str = cr.getValueAsString();
                                String b2str = new String((byte[])merged[c]);
                                
                                assertTrue (
                                    "Decoded blob: " + b1str + " " +
                                    "must be same as: " + b2str,
                                    b1str.equals (b2str)
                                );
                            }
                            else {
                                assertTrue (
                                    "Decoded value: " + cr.getValue() + " " +
                                    "must be same as: " + merged[c],
                                    cr.getValue().equals (merged[c])
                                );
                            }
                        }
                        else {
                            assertTrue (
                                "Must be null",
                                merged[c] == null
                            );
                        }
                    }
                    l++;
                }
                
                if (dr.isTransactionInfoRecord()) {
                    TransactionInfoRecord txr = (TransactionInfoRecord)dr;
                    
                    assertTrue (
                        "Expecting 1 TX per INSERT and LOB write",
                        txr.getRecordCount() == 1
                    );
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
        finally {
            if (plog != null) {
                plog.close();
            }
        }
    }
    
    @Test
    public void testMergeMultiPartUpdate () {
        final int PLOG_WITH_LCR_UPDATE_ID = 24;
        final int PLOG_WITH_LCR_UPDATE_TIMESTAMP = 1467163548;
        final String PLOG_WITH_LCR_UPDATE_FILENAME = "24.plog.1467163548";
        final String PLOG_LOCATION = "/data/mine/plog_single_lobupdate";
        
        URL resURL = this.getClass().getResource(PLOG_LOCATION);
            
        if (resURL == null) {
            fail ("Mine test path resource is not setup correctly");
        }

        String plogDir = resURL.getFile().toString();

        String fileName = plogDir + "/" + PLOG_WITH_LCR_UPDATE_FILENAME;
        PlogFile plog = null;

        try {
            /* test merging */
            plog = new PlogFile (
                PLOG_WITH_LCR_UPDATE_ID,
                PLOG_WITH_LCR_UPDATE_TIMESTAMP,
                fileName,
                DomainReader.builder()
                    .persistCriteria(persistCriteria)
                    .domainParsers(domainParsers)
                    .mergeMultiPartRecords(true)
                    .build()
            );
            plog.open ();

            PlogStreamReader reader = plog.getReader();
            
            /* for testing make flush size 100, but PLOG should contain only 1
             * LCR data record
             */
            reader.setFlushSize(100);

            while (!reader.isDone()) {
                reader.read();
            }

            List<DomainRecord> drs = reader.flush();
            
            assertTrue (
                "Expecting 2 LCR (LOB records merged with INSERT) and " +
                "2 TXs, but found " + drs.size(), 
                drs.size() == 2 + 2
            );
            
            int l = 0;
            
            for (DomainRecord dr : drs) {
                logger.info (dr.toJSONString());
                
                if (dr.isChangeRowRecord()) {
                    ChangeRowRecord lcr = (ChangeRowRecord)dr;
                    
                    assertTrue (
                        "Merged UPDATE only",
                        lcr.getAction().equals (ChangeAction.UPDATE)
                    );
                    
                    logger.info ("Testing LCR number: " + l);
                    Object[] merged = (Object[])MERGED_UPDATE_WITH_LOB_SQL[l];
                    
                    for (int c = 0; c < lcr.getColumnValues().size(); c++) {
                        ColumnValue cr = lcr.getColumnValues().get(c);
                        
                        if (cr != null && cr.getValue() != null) {
                            logger.info (cr.toString());
                            
                            if (cr.getValue() instanceof SerialBlob) {
                                String b1str = cr.getValueAsString();
                                String b2str = new String((byte[])merged[c]);
                                
                                assertTrue (
                                    "Decoded blob: " + b1str + " " +
                                    "must be same as: " + b2str,
                                    b1str.equals (b2str)
                                );
                            }
                            else {
                                assertTrue (
                                    "Decoded value: " + cr.getValue() + " " +
                                    "must be same as: " + merged[c],
                                    cr.getValue().equals (merged[c])
                                );
                            }
                        }
                        else {
                            assertTrue (
                                "Expecting column: " + c + "  value: " +
                                merged[c] + " but got NULL",
                                merged[c] == null
                            );
                        }
                    }
                    l++;
                }
                

                if (dr.isTransactionInfoRecord()) {
                    TransactionInfoRecord txr = (TransactionInfoRecord)dr;
                    
                    assertTrue (
                        "Expecting 1 TX per INSERT and LOB write",
                        txr.getRecordCount() == 1
                    );
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
        finally {
            if (plog != null) {
                plog.close();
            }
        }
    }
    
    /* single transaction info parser for aggregating */
    TransactionInfoParser txParser = new TransactionInfoParser();
    
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
                    new MetaDataParser()
                }
            );
            put (
                EntryType.ETYPE_LCR_DATA,
                new DomainParser[] {
                    new ChangeRowParser(),
                    txParser
                }
            );
            put (
                EntryType.ETYPE_TRANSACTIONS,
                new DomainParser[] { 
                    txParser
                }
            );
    }};
    
    @SuppressWarnings("serial")
    final Map<EntrySubType, Boolean> persistent = 
        new HashMap<EntrySubType, Boolean> () {{
            put (EntrySubType.ESTYPE_LCR_INSERT, true);
            put (EntrySubType.ESTYPE_LCR_UPDATE, true);
            put (EntrySubType.ESTYPE_LCR_DELETE, true);
            put (EntrySubType.ESTYPE_LCR_LOB_WRITE, true);
            put (EntrySubType.ESTYPE_LCR_LOB_ERASE, true);
            put (EntrySubType.ESTYPE_LCR_LOB_TRIM, true);
    }};
    
    TypeCriteria<EntrySubType> persistCriteria = 
        new TypeCriteria<EntrySubType> (persistent);
}

