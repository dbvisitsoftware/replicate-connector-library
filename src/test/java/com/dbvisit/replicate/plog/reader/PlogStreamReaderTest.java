package com.dbvisit.replicate.plog.reader;

import static org.junit.Assert.*;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.plog.domain.ChangeAction;
import com.dbvisit.replicate.plog.domain.ColumnDataType;
import com.dbvisit.replicate.plog.domain.ColumnValue;
import com.dbvisit.replicate.plog.domain.DomainRecord;
import com.dbvisit.replicate.plog.domain.DomainRecordType;
import com.dbvisit.replicate.plog.domain.HeaderRecord;
import com.dbvisit.replicate.plog.domain.ChangeRowRecord;
import com.dbvisit.replicate.plog.domain.TransactionInfoRecord;
import com.dbvisit.replicate.plog.domain.ReplicateOffset;
import com.dbvisit.replicate.plog.domain.parser.DomainParser;
import com.dbvisit.replicate.plog.domain.parser.ChangeRowParser;
import com.dbvisit.replicate.plog.domain.parser.TransactionInfoParser;
import com.dbvisit.replicate.plog.domain.parser.MetaDataParser;
import com.dbvisit.replicate.plog.domain.parser.ProxyDomainParser;
import com.dbvisit.replicate.plog.file.PlogFile;
import com.dbvisit.replicate.plog.format.EntrySubType;
import com.dbvisit.replicate.plog.format.EntryType;
import com.dbvisit.replicate.plog.metadata.Column;
import com.dbvisit.replicate.plog.metadata.DDLMetaData;
import com.dbvisit.replicate.plog.reader.criteria.AndCriteria;
import com.dbvisit.replicate.plog.reader.criteria.InternalDDLFilterCriteria;
import com.dbvisit.replicate.plog.reader.criteria.SchemaOffsetCriteria;
import com.dbvisit.replicate.plog.reader.criteria.TypeCriteria;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.nio.file.Path;

/**
 * Test PlogStreamReader
 */
public class PlogStreamReaderTest {
    private static final Logger logger = LoggerFactory.getLogger(
        PlogStreamReaderTest.class
    );
    
    /* for this test we persist all entry records */
    @Test
    public void testReadEmptyPLOGStream() {
        try {
            InputStream is = new ByteArrayInputStream (emptyPLOGByteArray);
            DataInputStream dis = new DataInputStream (is);
            
            DomainReader r = DomainReader.builder()
                .persistCriteria(persistCriteria)
                .domainParsers(domainParsers)
                .flushLastTransactions(true)
                .build();
            
            /* make a fake PLOG */
            PlogFile p = new PlogFile(r) {
                public boolean canUse() {
                    return true;
                }
                public boolean isCompact() {
                    return true;
                }
                public String getFullPath() {
                    return "/dev/null";
                }
                public String getFileName() {
                    return "mock-plog";
                }
            };
            p.setHeader (new HeaderRecord());
            p.getHeader().setCompactEncoding(true);
            
            PlogStreamReader psr = new PlogStreamReader(p, r, dis);
            psr.prepare();
            
            assertTrue (
                "Expecting valid PLOG stream",
                p.isValid()
            );
            
            assertTrue (
                "Expecting stream offset > " + PlogFile.PLOG_DATA_BYTE_OFFSET +
                ", got: " + psr.getOffset(),
                psr.getOffset() >= PlogFile.PLOG_DATA_BYTE_OFFSET
            );
            
            int count = 0;
            while (!psr.isDone()) {
                count = psr.read();
            }
            
            assertTrue (
                "Expect 0 record to be read, got: " + count,
                count == 0
            );
            
            List <DomainRecord> drs = psr.flush();
            
            assertTrue (
                "Expecting empty parsed data batch, got: " + drs.size(),
                drs.isEmpty()
            );
            
            drs = psr.flush();
            
            assertTrue ("No records read", drs.isEmpty());
            
            assertTrue (
                "Footer has been read, reader must be done",
                psr.isDone()
            );
            
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testReadDDLDataPLOGStream() {
        try {
            InputStream is = new ByteArrayInputStream (ddlDataPLOGByteArray);
            DataInputStream dis = new DataInputStream (is);
            
            DomainReader r = DomainReader.builder()
                .persistCriteria(persistCriteria)
                .domainParsers(domainParsers)
                .flushLastTransactions(true)
                .build();
                
            /* make a fake PLOG */
            PlogFile p = new PlogFile(r) {
                public boolean canUse() {
                    return true;
                }
                public boolean isCompact() {
                    return true;
                }
                public String getFullPath() {
                    return "/dev/null";
                }
                public String getFileName() {
                    return "mock-plog";
                }
            };
            p.setHeader (new HeaderRecord());
            p.getHeader().setCompactEncoding(true);

            PlogStreamReader psr = new PlogStreamReader(p, r, dis);
            psr.prepare();
            
            assertTrue (
                "Expecting valid PLOG stream",
                p.isValid()
            );
            
            assertTrue (
                "Expecting stream offset > " + PlogFile.PLOG_DATA_BYTE_OFFSET +
                ", got: " + psr.getOffset(),
                psr.getOffset() >= PlogFile.PLOG_DATA_BYTE_OFFSET
            );
            
            int count = 0;
            List<DomainRecord> drs = null;
            
            while (!psr.isDone()) {
                count = psr.read();
            }
            
            assertTrue (
                "Footer has been read, reader must be done",
                psr.isDone()
            );
            
            /* flush all records in byte stream */
            drs = psr.flush();
            
            assertTrue (
                "Number of records read: " + count + " must be same as number" +
                " of LCRs flushed: " + drs.size(),
                count == drs.size()
            );
            
            /* PLOG byte array has been hand crafted to have 4 records
             * associated with the following DDL
             * 
             * CREATE TABLE SOE.UNITTEST (
             *     ID NUMBER(6), 
             *     TEST_NAME VARCHAR2 (255)
             * );
             * 
             * 
             * 1. meta data JSON LCR that holds the schema definition
             * 2. NO OP LCR for DDL
             * 3. DDL LCR
             * 4. Footer <- never emitted
             */
            final int COUNT = 3;
            final DomainRecordType[] TYPES = new DomainRecordType[] {
                DomainRecordType.METADATA_RECORD,
                DomainRecordType.CHANGEROW_RECORD,
                DomainRecordType.CHANGEROW_RECORD
            };
            final String SCHEMA_NAME = "SOE.UNITTEST";
            final int NUM_COLUMNS = 2;
            final Column ID_COLUMN = new Column (
                1,
                "ID",
                "NUMBER",
                6,
                0,
                true
            );
            final Column TESTNAME_COLUMN = new Column (
                2,
                "TEST_NAME",
                "VARCHAR2",
                -1,
                -1,
                true
            );
            
            assertTrue (
                "Expecting: " + COUNT + " records, found: " + drs.size(),
                drs.size() == COUNT
            );
            
            for (int l = 0; l < COUNT; l++) {
                DomainRecord dr = drs.get(l);
                
                assertTrue (
                    "Expecting domain record of type: " + TYPES[l] + 
                    " but found: " + dr.getDomainRecordType(),
                    dr.getDomainRecordType().equals (TYPES[l])
                );
                
                logger.info (dr.toJSONString());
            }
            
            /* the PLOG should contain a schema definition for this table */
            assertTrue (
                "PLOG should contain cache for schema: " + SCHEMA_NAME,
                p.getSchemas().containsKey(SCHEMA_NAME)
            );
            
            DDLMetaData md = p.getSchemas().get(SCHEMA_NAME);
            
            assertTrue (
                "Expecting 2 columns, found: " + md.getColumns().size(),
                md.getColumns().size() == NUM_COLUMNS
            );
            
            Column col1 = md.getTableColumns().get(0);
            
            logger.info ("Column 1: " + col1.toString());
            
            assertTrue (
                "Expecting first column: " + ID_COLUMN.toString() + 
                ", found: " + col1.toString(),
                col1.toCmpString().equals (ID_COLUMN.toCmpString())
            );
            
            Column col2 = md.getTableColumns().get(1);
            
            logger.info ("Column 2: " + col2.toString());
            
            assertTrue (
                "Expecting second column: " + TESTNAME_COLUMN.toString() + 
                ", found: " + col2.toString(),
                col2.toCmpString().equals(TESTNAME_COLUMN.toCmpString())
            );
            
            psr.close();
            
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testReadInsertDataPLOGStream() {
        try {
            InputStream is = new ByteArrayInputStream (insertDataPLOGByteArray);
            DataInputStream dis = new DataInputStream (is);
            
            DomainReader r = DomainReader.builder()
                .persistCriteria(persistCriteria)
                .domainParsers(domainParsers)
                .flushLastTransactions(true)
                .build();
            /* make a fake PLOG */
            PlogFile p = new PlogFile(r) {
                public boolean canUse() {
                    return true;
                }
                public boolean isCompact() {
                    return true;
                }
                public String getFullPath() {
                    return "/dev/null";
                }
                public String getFileName() {
                    return "mock-plog";
                }
            };
            p.setHeader (new HeaderRecord());
            p.getHeader().setCompactEncoding(true);

            PlogStreamReader psr = new PlogStreamReader(p, r, dis);
            psr.prepare();
            
            assertTrue (
                "Expecting valid PLOG stream",
                p.isValid()
            );
            
            assertTrue (
                "Expecting stream offset > " + PlogFile.PLOG_DATA_BYTE_OFFSET +
                ", got: " + psr.getOffset(),
                psr.getOffset() >= PlogFile.PLOG_DATA_BYTE_OFFSET
            );
            
            List<DomainRecord> drs = null;
            while (!psr.isDone()) {
                psr.read();
            }
            
            assertTrue (
                "Footer has been read, reader must be done",
                psr.isDone()
            );
            
            drs = psr.flush();
            
            /* PLOG byte array has been hand crafted to have 4 records
             * associated with the following 
             *
             * INSERT INTO SOE.UNITTEST VALUES (1, 'TEST INSERT');
             */
            final int COUNT = 4;
            final DomainRecordType[] TYPES = new DomainRecordType[] {
                DomainRecordType.METADATA_RECORD,
                DomainRecordType.CHANGEROW_RECORD,
                DomainRecordType.CHANGEROW_RECORD,
                DomainRecordType.TRANSACTION_INFO_RECORD
            };
            final String SCHEMA_NAME = "SOE.UNITTEST";
            final ChangeAction RECORD_TYPE = ChangeAction.INSERT;
            final int DATA_LCR_IDX = 2;
            final int NUM_COLUMNS = 2;
            final ColumnValue ID_RECORD = new ColumnValue (
                1,
                ColumnDataType.NUMBER,
                "ID",
                1,
                true
            );
            final ColumnValue TESTNAME_RECORD = new ColumnValue (
                2,
                ColumnDataType.VARCHAR2,
                "TEST_NAME",
                "TEST INSERT",
                true
            );
            
            for (int l = 0; l < COUNT; l++) {
                DomainRecord dr = drs.get(l);
                
                assertTrue (
                    "Expecting domain record of type: " + TYPES[l] + 
                    " but found: " + dr.getDomainRecordType(),
                    dr.getDomainRecordType().equals (TYPES[l])
                );
                
                logger.info (dr.toJSONString());
            }
            
            /* the PLOG should contain a schema definition for this table */
            assertTrue (
                "PLOG should contain cache for schema: " + SCHEMA_NAME,
                p.getSchemas().containsKey(SCHEMA_NAME)
            );
            
            ChangeRowRecord dataLCR = 
                (ChangeRowRecord)drs.get(DATA_LCR_IDX);
            
            assertTrue (
                "Expecting " + RECORD_TYPE + " LCR, found: " +
                dataLCR.getAction(),
                dataLCR.getAction().equals (RECORD_TYPE)
            );
            
            List <ColumnValue> crs = dataLCR.getColumnValues();
            
            assertTrue (
                "Expecting " + NUM_COLUMNS + " of data columns, found: " +
                crs.size(),
                crs.size() == NUM_COLUMNS
            );

            ColumnValue rec1 = crs.get(0);
            
            logger.info ("Record 1: " + rec1.toString());
            
            assertTrue (
                "Expecting first column record: " + ID_RECORD.toString() + 
                ", found: " + rec1.toString(),
                rec1.toString().equals (ID_RECORD.toString())
            );
            
            ColumnValue rec2 = crs.get(1);
            
            logger.info ("Record 2: " + rec2.toString());
            
            assertTrue (
                "Expecting second column record: " + TESTNAME_RECORD.toString() + 
                ", found: " + rec2.toString(),
                rec2.toString().equals (TESTNAME_RECORD.toString())
            );
            
            /* last record is tx info, check that it was aggregated from only
             * one data LCR */
            TransactionInfoRecord txInfo = 
                (TransactionInfoRecord) drs.get (COUNT - 1);
            
            assertTrue(txInfo.getId().equals (dataLCR.getTransactionId()));
            assertTrue(txInfo.getStartPlogId() == dataLCR.getPlogId());
            assertTrue(txInfo.getEndPlogId() == dataLCR.getPlogId());
            assertTrue(txInfo.getStartRecordId() <= dataLCR.getId());
            assertTrue(txInfo.getEndRecordId() >= dataLCR.getId());
            assertTrue(txInfo.getStartSCN() <= dataLCR.getSystemChangeNumber());
            assertTrue(txInfo.getEndSCN() >= dataLCR.getSystemChangeNumber());
            assertTrue(txInfo.getStartTime().equals(dataLCR.getTimestamp()));
            assertTrue(txInfo.getEndTime().equals(dataLCR.getTimestamp()));
            assertTrue(txInfo.getRecordCount() == 1);
            
            psr.close();
            
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testReadUpdateDataPLOGStream() {
        try {
            InputStream is = new ByteArrayInputStream (updateDataPLOGByteArray);
            DataInputStream dis = new DataInputStream (is);
            
            DomainReader r = DomainReader.builder()
                .persistCriteria(persistCriteria)
                .domainParsers(domainParsers)
                .build();
            /* make a fake PLOG */
            PlogFile p = new PlogFile(r) {
                public boolean canUse() {
                    return true;
                }
                public boolean isCompact() {
                    return true;
                }
                public String getFullPath() {
                    return "/dev/null";
                }
                public String getFileName() {
                    return "mock-plog";
                }
            };
            p.setHeader (new HeaderRecord());
            p.getHeader().setCompactEncoding(true);

            PlogStreamReader psr = new PlogStreamReader(p, r, dis);
            psr.prepare();
            
            assertTrue (
                "Expecting valid PLOG stream",
                p.isValid()
            );
            
            assertTrue (
                "Expecting stream offset > " + PlogFile.PLOG_DATA_BYTE_OFFSET +
                ", got: " + psr.getOffset(),
                psr.getOffset() >= PlogFile.PLOG_DATA_BYTE_OFFSET
            );
            
            List<DomainRecord> drs = null;
            while (!psr.isDone()) {
                psr.read();
            }
            
            assertTrue (
                "Footer has been read, reader must be done",
                psr.isDone()
            );
            
            drs = psr.flush();

            /* PLOG byte array has been hand crafted to have 4 records
             * associated with the following 
             *
             * UPDATE SOE.UNITTEST SET TEST_NAME = 'TEST UPDATE';
             */
            final int COUNT = 3;
            final DomainRecordType[] TYPES = new DomainRecordType[] {
                DomainRecordType.METADATA_RECORD,
                DomainRecordType.CHANGEROW_RECORD,
                DomainRecordType.CHANGEROW_RECORD
            };
            final String SCHEMA_NAME = "SOE.UNITTEST";
            final ChangeAction RECORD_TYPE = ChangeAction.UPDATE;
            final int DATA_LCR_IDX = 2;
            final int NUM_COLUMNS = 2;
            final ColumnValue ID_RECORD = new ColumnValue (
                1,
                ColumnDataType.NUMBER,
                "ID",
                1,
                false
            );
            final ColumnValue TESTNAME_RECORD = new ColumnValue (
                2,
                ColumnDataType.VARCHAR2,
                "TEST_NAME",
                "TEST UPDATE",
                false
            );
            
            for (int l = 0; l < COUNT; l++) {
                DomainRecord dr = drs.get(l);
                
                assertTrue (
                    "Expecting domain record of type: " + TYPES[l] + 
                    " but found: " + dr.getDomainRecordType(),
                    dr.getDomainRecordType().equals (TYPES[l])
                );
                
                logger.info (dr.toJSONString());
            }
            
            /* the PLOG should contain a schema definition for this table */
            assertTrue (
                "PLOG should contain cache for schema: " + SCHEMA_NAME,
                p.getSchemas().containsKey(SCHEMA_NAME)
            );
            
            ChangeRowRecord dataLCR = 
                (ChangeRowRecord)drs.get(DATA_LCR_IDX);
            
            assertTrue (
                "Expecting " + RECORD_TYPE + " LCR, found: " +
                dataLCR.getAction(),
                dataLCR.getAction().equals (RECORD_TYPE)
            );
            
            List <ColumnValue> crs = dataLCR.getColumnValues();
            
            assertTrue (
                "Expecting " + NUM_COLUMNS + " of data columns, found: " +
                crs.size(),
                crs.size() == NUM_COLUMNS
            );

            ColumnValue rec1 = crs.get(0);
            
            logger.info ("Record 1: " + rec1.toString());
            
            assertTrue (
                "Expecting first column record: " + ID_RECORD.toString() + 
                ", found: " + rec1.toString(),
                rec1.toString().equals (ID_RECORD.toString())
            );
            
            ColumnValue rec2 = crs.get(1);
            
            logger.info ("Record 2: " + rec2.toString());
            
            assertTrue (
                "Expecting second column record: " + TESTNAME_RECORD.toString() + 
                ", found: " + rec2.toString(),
                rec2.toString().equals (TESTNAME_RECORD.toString())
            );
            
            psr.close();
            
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testReadDeleteDataPLOGStream() {
        try {
            InputStream is = new ByteArrayInputStream (deleteDataPLOGByteArray);
            DataInputStream dis = new DataInputStream (is);
            
            DomainReader r = DomainReader.builder()
                .persistCriteria(persistCriteria)
                .domainParsers(domainParsers)
                .build();
            
            /* make a fake PLOG */
            PlogFile p = new PlogFile(r) {
                public boolean canUse() {
                    return true;
                }
                public boolean isCompact() {
                    return true;
                }
                public String getFullPath() {
                    return "/dev/null";
                }
                public String getFileName() {
                    return "mock-plog";
                }
            };
            p.setHeader (new HeaderRecord());
            p.getHeader().setCompactEncoding(true);

            PlogStreamReader psr = new PlogStreamReader(p, r, dis);
            psr.prepare();
            
            assertTrue (
                "Expecting valid PLOG stream",
                p.isValid()
            );
            
            assertTrue (
                "Expecting stream offset > " + PlogFile.PLOG_DATA_BYTE_OFFSET +
                ", got: " + psr.getOffset(),
                psr.getOffset() >= PlogFile.PLOG_DATA_BYTE_OFFSET
            );
            

            List<DomainRecord> drs = null;
            while (!psr.isDone()) {
                psr.read();
            }
            
            assertTrue (
                "Footer has been read, reader must be done",
                psr.isDone()
            );
            
            drs = psr.flush();
            
            /* PLOG byte array has been hand crafted to have 4 records
             * associated with the following 
             *
             * DELETE FROM SOE.UNITTEST WHERE ID = 1;
             */
            final int COUNT = 3;
            final DomainRecordType[] TYPES = new DomainRecordType[] {
                DomainRecordType.METADATA_RECORD,
                DomainRecordType.CHANGEROW_RECORD,
                DomainRecordType.CHANGEROW_RECORD
            };
            final String SCHEMA_NAME = "SOE.UNITTEST";
            final ChangeAction RECORD_TYPE = ChangeAction.DELETE;
            final int DATA_LCR_IDX = 2;
            final int NUM_COLUMNS = 2;
            final ColumnValue ID_RECORD = new ColumnValue (
                1,
                ColumnDataType.NUMBER,
                "ID",
                1,
                false
            );
            final ColumnValue TESTNAME_RECORD = new ColumnValue (
                2,
                ColumnDataType.VARCHAR2,
                "TEST_NAME",
                "TEST UPDATE",
                false
            );
            
            for (int l = 0; l < COUNT; l++) {
                DomainRecord dr = drs.get(l);
                
                assertTrue (
                    "Expecting domain record of type: " + TYPES[l] + 
                    " but found: " + dr.getDomainRecordType(),
                    dr.getDomainRecordType().equals (TYPES[l])
                );
                
                logger.info (dr.toJSONString());
            }
            
            /* the PLOG should contain a schema definition for this table */
            assertTrue (
                "PLOG should contain cache for schema: " + SCHEMA_NAME,
                p.getSchemas().containsKey(SCHEMA_NAME)
            );
            
            ChangeRowRecord dataLCR = 
                (ChangeRowRecord)drs.get(DATA_LCR_IDX);
            
            assertTrue (
                "Expecting " + RECORD_TYPE + " LCR, found: " +
                dataLCR.getAction(),
                dataLCR.getAction().equals (RECORD_TYPE)
            );
            
            List <ColumnValue> crs = dataLCR.getColumnValues();
            
            assertTrue (
                "Expecting " + NUM_COLUMNS + " of data columns, found: " +
                crs.size(),
                crs.size() == NUM_COLUMNS
            );

            ColumnValue rec1 = crs.get(0);
            
            logger.info ("Record 1: " + rec1.toString());
            
            assertTrue (
                "Expecting first column record: " + ID_RECORD.toString() + 
                ", found: " + rec1.toString(),
                rec1.toString().equals (ID_RECORD.toString())
            );
            
            ColumnValue rec2 = crs.get(1);
            
            logger.info ("Record 2: " + rec2.toString());
            
            assertTrue (
                "Expecting second column record: " + TESTNAME_RECORD.toString() + 
                ", found: " + rec2.toString(),
                rec2.toString().equals (TESTNAME_RECORD.toString())
            );

            
            psr.close();
            
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    /* Test reading stream which references included PLOGs, each with 18
     * data records
     */
    @Test
    public void testReadPlogAndIncludedPlogStream () {
        final int PLOG_ID = 492;
        final int PLOG_LOAD_RECORDS = 36;
        final String LOAD_TABLE_1 = "SCOTT.TEST1";
        final int LOAD_TABLE_1_RECORDS = 18;
        final String LOAD_TABLE_2 = "SCOTT.TEST2";
        final int LOAD_TABLE_2_RECORDS = 18;
        
        try {
            InputStream is = new ByteArrayInputStream (includePLOGDataByteArray);
            DataInputStream dis = new DataInputStream (is);
            
            URL resURL = this.getClass().getResource("/data/mine/plog_load_set");
            
            if (resURL == null) {
                fail ("Mine test path resource is not setup correctly");
            }
            
            DomainReader r = DomainReader.builder()
                .persistCriteria(persistCriteria)
                .domainParsers(domainParsers)
                .mergeMultiPartRecords(true)
                .flushLastTransactions(true)
                .build();

            /* make a fake PLOG */
            PlogFile p = new PlogFile(r, dis) {
                public boolean canUse() {
                    return true;
                }
                public boolean isCompact() {
                    return true;
                }
                public String getFullPath() {
                    return "/dev/null";
                }
            };
            p.setId(492);
            p.setTimestamp(1473711131);
            p.setFileName("429.plog.1473711131");
            p.setBaseDirectory(resURL.getFile());
            p.setHeader (new HeaderRecord());
            p.getHeader().setCompactEncoding(true);

            final PlogStreamReader psr = p.getReader();
            psr.prepare();
            
            assertTrue (
                "Expecting valid PLOG stream",
                p.isValid()
            );
            
            assertTrue (
                "Expecting stream offset > " + PlogFile.PLOG_DATA_BYTE_OFFSET +
                ", got: " + psr.getOffset(),
                psr.getOffset() >= PlogFile.PLOG_DATA_BYTE_OFFSET
            );
            
            List<DomainRecord> drs = null;
            while (!psr.isDone()) {
                psr.read();
            }
            
            assertTrue (
                "Footer has been read, reader must be done",
                psr.isDone()
            );
            
            drs = psr.flush();
            
            Map<Integer, Integer> plogDataCounts = 
                new HashMap<Integer, Integer>();
            Map<String, Integer> tableLoadCounts =
                new HashMap<String, Integer>();
                
            for (DomainRecord dr : drs) {
                logger.info (dr.toJSONString());
                
                if (dr.isDataRecord()) {
                    int plogId = dr.getParentId();
                    
                    if (!plogDataCounts.containsKey(plogId)) {
                        plogDataCounts.put (plogId, 0);
                    }
                    int count = plogDataCounts.get (plogId);
                    plogDataCounts.put (plogId, ++count);
                    
                    String schema = dr.getRecordSchema();
                    
                    if (!tableLoadCounts.containsKey (schema)) {
                        tableLoadCounts.put (schema, 0);
                    }
                    
                    int loadCount = tableLoadCounts.get (schema);
                    tableLoadCounts.put (schema, ++loadCount);
                }
            }
            
            psr.close();
            
            assertTrue (
                "Expecting PLOG: " + PLOG_ID + " with: " + PLOG_LOAD_RECORDS +
                " records , got: " + plogDataCounts.toString(),
                plogDataCounts.containsKey(PLOG_ID) &&
                plogDataCounts.get(PLOG_ID) == PLOG_LOAD_RECORDS
            );
            
            assertTrue (
                "Expecting LOAD TABLE: " + LOAD_TABLE_1 + " with: " +  
                LOAD_TABLE_1_RECORDS + " records and LOAD TABLE: "  +
                LOAD_TABLE_2 + " with: " + LOAD_TABLE_2_RECORDS     +
                " records, got: " + tableLoadCounts.toString(),
                tableLoadCounts.containsKey (LOAD_TABLE_1) &&
                tableLoadCounts.get (LOAD_TABLE_1) == LOAD_TABLE_1_RECORDS &&
                tableLoadCounts.containsKey (LOAD_TABLE_2) &&
                tableLoadCounts.get (LOAD_TABLE_2) == LOAD_TABLE_2_RECORDS
            );
            
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }

    /* Test reading stream which references included PLOGs by applying
     * schema offset criteria to filter first IFILE records, this is
     * to test that all sub-stream records are treated as if they
     * are encoded at parent file offset of IFILE
     */
    @Test
    public void testReadPlogAndIncludedPlogStreamBySchemaOffset () {
        final int PLOG_ID = 492;
        /* one table of 18 records only */
        final int PLOG_LOAD_RECORDS = 18;
        final String LOAD_TABLE_2 = "SCOTT.TEST2";
        final int LOAD_TABLE_2_RECORDS = 18;
        
        @SuppressWarnings("serial")
        final Map<String, ReplicateOffset> schemaOffsets = 
            new HashMap<String, ReplicateOffset> () {{
                put (
                    "SCOTT.TEST1", 
                    /* plog 492, timestamp 1473711131, offset 696 */
                    new ReplicateOffset (
                        2114597620763L,
                        696L
                    )
                );
                put (
                    "SCOTT.TEST2", 
                    /* plog 492, timestamp 1473711131, offset 696 */
                    new ReplicateOffset (
                        2114597620763L,
                        696L
                    )
                );
        }};
        
        SchemaOffsetCriteria<EntrySubType> parseCriteria =
            new SchemaOffsetCriteria<EntrySubType>(schemaOffsets);
        
        try {
            InputStream is = new ByteArrayInputStream (includePLOGDataByteArray);
            DataInputStream dis = new DataInputStream (is);
            
            URL resURL = this.getClass().getResource("/data/mine/plog_load_set");
            
            if (resURL == null) {
                fail ("Mine test path resource is not setup correctly");
            }
            
            DomainReader r = DomainReader.builder()
                .persistCriteria(persistCriteria)
                .domainParsers(domainParsers)
                .parseCriteria(parseCriteria)
                .mergeMultiPartRecords(true)
                .flushLastTransactions(true)
                .build();
            
            /* make a fake PLOG */
            PlogFile p = new PlogFile(r, dis) {
                public boolean canUse() {
                    return true;
                }
                public boolean isCompact() {
                    return true;
                }
                public String getFullPath() {
                    return "/dev/null";
                }
            };
            p.setId(492);
            p.setTimestamp(1473711131);
            p.setFileName("429.plog.1473711131");
            p.setBaseDirectory(resURL.getFile());
            p.setHeader (new HeaderRecord());
            p.getHeader().setCompactEncoding(true);

            // need to inject this final psr into Plog
            final PlogStreamReader psr = p.getReader();
            psr.prepare();

            assertTrue (
                "Expecting valid PLOG stream",
                p.isValid()
            );
            
            assertTrue (
                "Expecting stream offset > " + PlogFile.PLOG_DATA_BYTE_OFFSET +
                ", got: " + psr.getOffset(),
                psr.getOffset() >= PlogFile.PLOG_DATA_BYTE_OFFSET
            );
            
            List<DomainRecord> drs = null;
            while (!psr.isDone()) {
                psr.read();
            }
            
            assertTrue (
                "Footer has been read, reader must be done",
                psr.isDone()
            );
            
            drs = psr.flush();
            
            Map<Integer, Integer> plogDataCounts = 
                new HashMap<Integer, Integer>();
            Map<String, Integer> tableLoadCounts =
                new HashMap<String, Integer>();
                
            for (DomainRecord dr : drs) {
                if (dr.isDataRecord()) {
                    logger.debug (
                        "[" + dr.getRecordSchema() + "]: " + 
                        dr.toJSONString()
                    );
                    
                    int plogId = dr.getParentId();
                    
                    if (!plogDataCounts.containsKey(plogId)) {
                        plogDataCounts.put (plogId, 0);
                    }
                    int count = plogDataCounts.get (plogId);
                    plogDataCounts.put (plogId, ++count);
                    
                    String schema = dr.getRecordSchema();
                    
                    if (!tableLoadCounts.containsKey (schema)) {
                        tableLoadCounts.put (schema, 0);
                    }
                    
                    int loadCount = tableLoadCounts.get (schema);
                    tableLoadCounts.put (schema, ++loadCount);
                }
            }
            
            psr.close();
            
            assertTrue (
                "Expecting PLOG: " + PLOG_ID + " with: " + PLOG_LOAD_RECORDS +
                " records , got: " + plogDataCounts.toString(),
                plogDataCounts.containsKey(PLOG_ID) &&
                plogDataCounts.get(PLOG_ID) == PLOG_LOAD_RECORDS
            );
            
            assertTrue (
                "Expecting 1 LOAD TABLE: "  + LOAD_TABLE_2 + 
                " with: " + LOAD_TABLE_2_RECORDS +
                " records, got: " + tableLoadCounts.toString(),
                tableLoadCounts.size() == 1 &&
                tableLoadCounts.containsKey (LOAD_TABLE_2) &&
                tableLoadCounts.get (LOAD_TABLE_2) == LOAD_TABLE_2_RECORDS
            );
            
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    /* NOT A TEST: use this to extract parts of a PLOG */
    @Test
    public void printFilteredPlogAsBytes () {
        try {
            @SuppressWarnings("serial")
            final Map<EntrySubType, Boolean> filter =
            new HashMap<EntrySubType, Boolean> () {{
                put (EntrySubType.ESTYPE_HEADER, true);
                put (EntrySubType.ESTYPE_FOOTER, true);
                put (EntrySubType.ESTYPE_LCR_PLOG_IFILE, true);
                put (EntrySubType.ESTYPE_LCR_PLOG_IFILE_STATS, true);
                put (EntrySubType.ESTYPE_LCR_INSERT, true);
                put (EntrySubType.ESTYPE_LCR_UPDATE, true);
                put (EntrySubType.ESTYPE_LCR_DELETE, true);
                put (EntrySubType.ESTYPE_LCR_LOB_WRITE, true);
                put (EntrySubType.ESTYPE_LCR_LOB_ERASE, true);
                put (EntrySubType.ESTYPE_LCR_LOB_TRIM, true);
                put (EntrySubType.ESTYPE_LCR_NOOP, true);
                put (EntrySubType.ESTYPE_LCR_DDL, true);
                put (EntrySubType.ESTYPE_DDL_JSON, true);
            }};
            
            int [] ids = new int[] {
            };
            
            String [] files = new String[] {
            };
            
            for (int i = 0; i < ids.length; i++) {
                int id = ids[i];
                String file = files[i];
                
                logger.info ("Filtering: " + file);
                
                PrintWriter writer = new PrintWriter(
                    "/tmp/" + file + ".txt", "UTF-8"
                );

                URL resURL = this.getClass().getResource(
                    "/data/mine/plog_export/" + file
                );

                if (resURL == null) {
                    fail ("Mine test path resource is not setup correctly");
                }

                Map<Long, Integer> offsets = 
                    new LinkedHashMap <Long, Integer>();
                
                DomainReader domainReader = DomainReader.builder()
                    .persistCriteria(persistCriteria)
                    .parseCriteria(
                        new AndCriteria<EntrySubType> (
                            new TypeCriteria<EntrySubType> (filter),
                            new InternalDDLFilterCriteria<EntrySubType>()
                        )
                    )
                    .domainParsers(domainParsers)
                    .mergeMultiPartRecords(true)
                    .flushLastTransactions(true)
                    .build();
                
                PlogFile plog = new PlogFile (
                    id,
                    0,
                    resURL.getFile(),
                    domainReader
                );
                plog.open ();
                
                final PlogStreamReader reader = plog.getReader();
                
                reader.setFlushSize(1);
                
                /* file header */
                offsets.put (0L, 16);
                /* control/header record */
                offsets.put (16L, 96);
                
                while (!reader.isDone()) {
                    reader.read();
                    if (reader.canFlush()) {
                        List<DomainRecord> drs = reader.flush();
                        DomainRecord dr = drs.get(0);
                    
                        logger.info (dr.toJSONString());
                    
                        offsets.put (
                            reader.getOffset() - dr.getRawRecordSize(), 
                            dr.getRawRecordSize()
                        );
                    }
                }
                
                logger.info (offsets.toString());
                
                plog.close();
                
                Path path = Paths.get(resURL.getFile());
                byte[] data = Files.readAllBytes(path);
                
                /* add footer last */
                offsets.put((long)data.length - 56L, 56);

                logger.info ("Read: " + data.length + " bytes");
                
                int b = 0;
                
                /* now filter the PLOG */
                for (long offset : offsets.keySet()) {
                    int len = offsets.get (offset);

                    logger.info ("Copying from :" + offset + " of length:" + len);

                    for (int f = (int)offset; f < offset + len; f++) {
                        if (b > 0) {
                            writer.print (", ");
                        }
                        
                        if (b % 8 == 0) {
                            writer.println (""); 
                        }
                        
                        writer.print (data[f]);
                        b++;
                    }
                    
                }
                    
                logger.info ("Copied: " + b + " bytes");

                writer.close();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    /* NOT A TEST: use this to extract LCRs from a PLOG */
    @Test
    public void printFilteredLCRsAsBytes () {
        try {
            @SuppressWarnings("serial")
            final Map<EntrySubType, Boolean> filter =
            new HashMap<EntrySubType, Boolean> () {{
                put (EntrySubType.ESTYPE_LCR_INSERT, true);
                put (EntrySubType.ESTYPE_LCR_LOB_WRITE, true);
                put (EntrySubType.ESTYPE_DDL_JSON, true);
                put (EntrySubType.ESTYPE_LCR_NOOP, true);
            }};
            
            /* to use add PLOG IDs */
            int [] ids = new int[] {
            };
            
            /* to use add PLOGs */
            String [] files = new String[] {
            };
            
            for (int i = 0; i < ids.length; i++) {
                int id = ids[i];
                String file = files[i];
                
                PrintWriter writer = new PrintWriter(
                    "/tmp/" + file + ".txt", "UTF-8"
                );

                URL resURL = this.getClass().getResource(
                    "/data/mine/plog_export/" + file
                );

                if (resURL == null) {
                    fail ("Mine test path resource is not setup correctly");
                }

                Map<Long, Integer> offsets = 
                    new LinkedHashMap <Long, Integer>();
                
                DomainReader domainReader = DomainReader.builder()
                    .parseCriteria(new TypeCriteria<EntrySubType> (filter))
                    .persistCriteria(persistCriteria)
                    .domainParsers(domainParsers)
                    .mergeMultiPartRecords(true)
                    .flushLastTransactions(true)
                    .build();
                
                PlogFile plog = new PlogFile (
                    id, 
                    0, 
                    resURL.getFile(), 
                    domainReader
                );
                
                plog.open ();
                
                PlogStreamReader reader = plog.getReader();
                
                reader.setFlushSize(1);
                while (!reader.isDone()) {
                    reader.read();
                    if (reader.canFlush()) {
                        List<DomainRecord> drs = reader.flush();
                        DomainRecord dr = drs.get(0);
                    
                        logger.info (dr.toJSONString());
                    
                        offsets.put (
                            reader.getOffset() - dr.getRawRecordSize(), 
                            dr.getRawRecordSize()
                        );
                    }
                }
                
                logger.info (offsets.toString());
                
                plog.close();
                
                Path path = Paths.get(resURL.getFile());
                byte[] data = Files.readAllBytes(path);

                logger.info ("Read: " + data.length + " bytes");
                
                int b = 0;
                
                /* now filter the PLOG */
                for (long offset : offsets.keySet()) {
                    int len = offsets.get (offset);

                    logger.info ("Copying from :" + offset + " of length:" + len);

                    for (int f = (int)offset; f < offset + len; f++) {
                        if (b > 0) {
                            writer.print (", ");
                        }
                        
                        if (b % 8 == 0) {
                            writer.println (""); 
                        }
                        
                        writer.print (data[f]);
                        b++;
                    }
                    
                }
                    
                logger.info ("Copied: " + b + " bytes");

                writer.close();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
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
            put (
                EntryType.ETYPE_LCR_PLOG,
                new DomainParser[] { 
                    new ProxyDomainParser() 
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
            put (EntrySubType.ESTYPE_LCR_NOOP, true);
            put (EntrySubType.ESTYPE_LCR_DDL, true);
            put (EntrySubType.ESTYPE_DDL_JSON, true);
            put (EntrySubType.ESTYPE_FOOTER, true);
            put (EntrySubType.ESTYPE_TRAN_AUDIT, true);
            put (EntrySubType.ESTYPE_TRAN_START, true);
            put (EntrySubType.ESTYPE_TRAN_COMMIT, true);
            put (EntrySubType.ESTYPE_TRAN_ROLLBACK, true);
            put (EntrySubType.ESTYPE_TRAN_ROLLBACK_TO_SAVEPOINT, true);
            put (EntrySubType.ESTYPE_LCR_PLOG_IFILE, true);
            put (EntrySubType.ESTYPE_LCR_PLOG_IFILE_STATS, false);
            
    }};
    
    TypeCriteria<EntrySubType> persistCriteria = 
        new TypeCriteria<EntrySubType> (persistent);
    
    /* byte array of PLOG */
    private byte [] emptyPLOGByteArray = new byte[] {
       80, 76, 79, 71, 10, 32, 13, 32, 
        1,  0,  0,  0,  1,  0,  0,  0,
        24, 0, 0, 0, 0, 0, 0, 0, 
        0, 0, 0, 0, 3, 0, 0, 0, 
        1, 0, 1, 0, 76, -34, 96, -90, 
        12, 0, 0, 0, 6, 0, 0, 0, 
        51, 70, 56, 50, 56, 51, 53, 52, 
        45, 53, 49, 70, 65, 45, 49, 49, 
        69, 54, 45, 65, 50, 70, 67, 45, 
        54, 50, 66, 66, 69, 70, 56, 65, 
        48, 55, 67, 55, 0, 0, 0, 0, 
        3, 0, 0, 0, 7, 0, 0, 0, 
        11, 0, 0, 0, 3, 0, 0, 0, 
        8, 0, 0, 0, 15, 0, 0, 0, 
       15,  0,  0,  0,  1,  0,  0,  0,
        4,  0,  0,  0,  5,  0,  0,  0,
        1,  0,  3,  0, 48, 48, 49, 46,
       48, 48, 50, 46, 48, 48, 51,  0, 
        3,  0,  0,  0, 16,  0,  2,  0,
      123,  0,  0,  0,  4,  0,  0,  0,
       16,  0,  2,  0, 52, 28,-36,-33, 
        2,  0,  0,  0,  8,  0,  0,  0,
        0,  0,  0,  0,  1,  0,  0,  0, 
        5,  0,  0,  0,  5,  0,  0,  0,
       80, 76, 79, 71, 32, 69, 78, 68,
        0,  0,  0,  0
    };

    /** 
     * raw plog byte array for
     * 
     * CREATE TABLE SOE.UNITTEST (
     *     ID NUMBER(6), 
     *     TEST_NAME VARCHAR2 (255)
     * );
     */
    private byte [] ddlDataPLOGByteArray = new byte [] {
        80, 76, 79, 71, 10, 32, 13, 32, 
        1, 0, 0, 0, 1, 0, 0, 0, 
        24, 0, 0, 0, 0, 0, 0, 0, 
        0, 0, 0, 0, 3, 0, 0, 0, 
        1, 0, 1, 0, 76, -34, 96, -90, 
        12, 0, 0, 0, 6, 0, 0, 0, 
        51, 70, 56, 50, 56, 51, 53, 52, 
        45, 53, 49, 70, 65, 45, 49, 49, 
        69, 54, 45, 65, 50, 70, 67, 45, 
        54, 50, 66, 66, 69, 70, 56, 65, 
        48, 55, 67, 55, 0, 0, 0, 0, 
        3, 0, 0, 0, 7, 0, 0, 0, 
        11, 0, 0, 0, 3, 0, 0, 0, 
        8, 0, 0, 0, 15, 0, 0, 0, 
        122, 0, 0, 0, 21, 0, 0, 
        0, 1, 0, 0, 0, 8, 0, 0, 
        0, 1, 0, 3, 0, 35, 68, 68, 
        76, 32, 97, 116, 32, 48, 48, 48, 
        48, 46, 48, 48, 48, 98, 56, 49, 
        98, 99, 0, 0, 0, 94, 0, 0, 
        0, 5, 0, 4, 0, 123, 10, 32, 
        34, 99, 111, 108, 117, 109, 110, 115, 
        34, 58, 32, 91, 10, 123, 10, 34, 
        99, 111, 108, 117, 109, 110, 73, 100, 
        34, 58, 32, 49, 44, 10, 34, 99, 
        111, 108, 117, 109, 110, 78, 97, 109, 
        101, 34, 58, 32, 34, 73, 68, 34, 
        44, 10, 34, 99, 111, 108, 117, 109, 
        110, 84, 121, 112, 101, 34, 58, 32, 
        34, 78, 85, 77, 66, 69, 82, 34, 
        44, 10, 34, 99, 111, 108, 117, 109, 
        110, 80, 114, 101, 99, 105, 115, 105, 
        111, 110, 34, 58, 32, 54, 44, 10, 
        34, 99, 111, 108, 117, 109, 110, 83, 
        99, 97, 108, 101, 34, 58, 32, 48, 
        44, 10, 34, 105, 115, 78, 117, 108, 
        108, 97, 98, 108, 101, 34, 58, 32, 
        116, 114, 117, 101, 10, 125, 44, 10, 
        123, 10, 34, 99, 111, 108, 117, 109, 
        110, 73, 100, 34, 58, 32, 50, 44, 
        10, 34, 99, 111, 108, 117, 109, 110, 
        78, 97, 109, 101, 34, 58, 32, 34, 
        84, 69, 83, 84, 95, 78, 65, 77, 
        69, 34, 44, 10, 34, 99, 111, 108, 
        117, 109, 110, 84, 121, 112, 101, 34, 
        58, 32, 34, 86, 65, 82, 67, 72, 
        65, 82, 50, 34, 44, 10, 34, 99, 
        111, 108, 117, 109, 110, 80, 114, 101, 
        99, 105, 115, 105, 111, 110, 34, 58, 
        32, 45, 49, 44, 10, 34, 99, 111, 
        108, 117, 109, 110, 83, 99, 97, 108, 
        101, 34, 58, 32, 45, 49, 44, 10, 
        34, 105, 115, 78, 117, 108, 108, 97, 
        98, 108, 101, 34, 58, 32, 116, 114, 
        117, 101, 10, 125, 10, 93, 44, 10, 
        32, 34, 111, 98, 106, 101, 99, 116, 
        73, 100, 34, 58, 32, 50, 48, 50, 
        53, 51, 44, 10, 34, 118, 97, 108, 
        105, 100, 83, 105, 110, 99, 101, 83, 
        67, 78, 34, 58, 32, 55, 53, 52, 
        49, 48, 55, 44, 10, 34, 115, 99, 
        104, 101, 109, 97, 78, 97, 109, 101, 
        34, 58, 32, 34, 83, 79, 69, 34, 
        44, 10, 34, 116, 97, 98, 108, 101, 
        78, 97, 109, 101, 34, 58, 32, 34, 
        85, 78, 73, 84, 84, 69, 83, 84, 
        34, 10, 125, 0, 0, 3, 0, 0, 
        0, 32, 0, 2, 0, 29, 79, 0, 
        0, 4, 0, 0, 0, 2, 0, 3, 
        0, -68, -127, 11, 0, 0, 0, 0, 
        0, 6, 0, 0, 0, 50, 0, 2, 
        0, 20, 0, 0, 0, 76, 0, 0, 
        0, -108, 1, 0, 0, 2, 0, 0, 
        0, 4, 0, 0, 0, 48, 0, 2, 
        0, -49, -106, -104, 0, 0, 0, 0, 
        0, 69, 0, 0, 0, 11, 0, 0, 
        0, 0, 2, 0, 0, 8, 0, 0, 
        0, 1, 0, 3, 0, 35, 68, 68, 
        76, 32, 97, 116, 32, 48, 48, 48, 
        48, 46, 48, 48, 48, 98, 56, 49, 
        98, 99, 0, 0, 0, 3, 0, 0, 
        0, 32, 0, 2, 0, 29, 79, 0, 
        0, 3, 0, 0, 0, 34, 0, 2, 
        0, 83, 79, 69, 0, 5, 0, 0, 
        0, 33, 0, 2, 0, 85, 78, 73, 
        84, 84, 69, 83, 84, 0, 0, 0, 
        0, 4, 0, 0, 0, 2, 0, 3, 
        0, -68, -127, 11, 0, 0, 0, 0, 
        0, 3, 0, 0, 0, 16, 0, 2, 
        0, 1, 0, 0, 0, 3, 0, 0, 
        0, 17, 0, 2, 0, 73, 68, 0, 
        0, 4, 0, 0, 0, 18, 0, 2, 
        0, 78, 85, 77, 66, 69, 82, 0, 
        0, 6, 0, 0, 0, 50, 0, 2, 
        0, 20, 0, 0, 0, 76, 0, 0, 
        0, -108, 1, 0, 0, 2, 0, 0, 
        0, 4, 0, 0, 0, 48, 0, 2, 
        0, -54, -106, -104, 0, 0, 0, 0, 
        0, 3, 0, 0, 0, 16, 0, 2, 
        0, 2, 0, 0, 0, 5, 0, 0, 
        0, 17, 0, 2, 0, 84, 69, 83, 
        84, 95, 78, 65, 77, 69, 0, 0, 
        0, 5, 0, 0, 0, 18, 0, 2, 
        0, 86, 65, 82, 67, 72, 65, 82, 
        50, 0, 0, 0, 0, 6, 0, 0, 
        0, 50, 0, 2, 0, 20, 0, 0, 
        0, 76, 0, 0, 0, -108, 1, 0, 
        0, 2, 0, 0, 0, 4, 0, 0, 
        0, 48, 0, 2, 0, -48, -106, -104, 
        0, 0, 0, 0, 0, 67, 0, 0, 
        0, 11, 0, 0, 0, 0, 1, 0, 
        0, 4, 0, 0, 0, 2, 0, 3, 
        0, -64, -127, 11, 0, 0, 0, 0, 
        0, 3, 0, 0, 0, 3, 0, 3, 
        0, 127, -39, -81, 54, 8, 0, 0, 
        0, 1, 0, 3, 0, 35, 68, 68, 
        76, 32, 97, 116, 32, 48, 48, 48, 
        48, 46, 48, 48, 48, 98, 56, 49, 
        98, 99, 0, 0, 0, 3, 0, 0, 
        0, 1, 0, 4, 0, 83, 89, 83, 
        0, 3, 0, 0, 0, 2, 0, 4, 
        0, 83, 89, 83, 0, 3, 0, 0, 
        0, 34, 0, 2, 0, 83, 79, 69, 
        0, 5, 0, 0, 0, 33, 0, 2, 
        0, 85, 78, 73, 84, 84, 69, 83, 
        84, 0, 0, 0, 0, 3, 0, 0, 
        0, 32, 0, 2, 0, 29, 79, 0, 
        0, 3, 0, 0, 0, 4, 0, 4, 
        0, 1, 0, 0, 0, 19, 0, 0, 
        0, 3, 0, 4, 0, 67, 82, 69, 
        65, 84, 69, 32, 84, 65, 66, 76, 
        69, 32, 83, 79, 69, 46, 85, 78, 
        73, 84, 84, 69, 83, 84, 32, 40, 
        73, 68, 32, 78, 85, 77, 66, 69, 
        82, 40, 54, 41, 44, 32, 84, 69, 
        83, 84, 95, 78, 65, 77, 69, 32, 
        86, 65, 82, 67, 72, 65, 82, 50, 
        32, 40, 50, 53, 53, 41, 41, 0, 
        0, 4, 0, 0, 0, 48, 0, 2, 
        0, -44, -106, -104, 0, 0, 0, 0, 
        0, 6, 0, 0, 0, 50, 0, 2, 
        0, 20, 0, 0, 0, 76, 0, 0, 
        0, -108, 1, 0, 0, 1, 0, 0, 
        0, 14, 0, 0, 0, 0, 0, 0, 
        0, 1, 0, 0, 0, 3, 0, 0, 
        0, 4, 0, 0, 0, 1, 0, 0, 
        0, 3, 0, 0, 0, 2, 0, 0, 
        0, -65, 76, 47, 42, 5, 0, 0, 
        0, 5, 0, 0, 0, 80, 76, 79, 
        71, 32, 69, 78, 68, 0, 0, 0, 
        0
    };
    
    private byte[] insertDataPLOGByteArray = new byte[] {
        80, 76, 79, 71, 10, 32, 13, 32, 
        1, 0, 0, 0, 1, 0, 0, 0, 
        24, 0, 0, 0, 0, 0, 0, 0, 
        0, 0, 0, 0, 3, 0, 0, 0, 
        1, 0, 1, 0, 76, -34, 96, -90, 
        12, 0, 0, 0, 6, 0, 0, 0, 
        51, 70, 56, 50, 56, 51, 53, 52, 
        45, 53, 49, 70, 65, 45, 49, 49, 
        69, 54, 45, 65, 50, 70, 67, 45, 
        54, 50, 66, 66, 69, 70, 56, 65, 
        48, 55, 67, 55, 0, 0, 0, 0, 
        3, 0, 0, 0, 7, 0, 0, 0, 
        11, 0, 0, 0, 3, 0, 0, 0, 
        8, 0, 0, 0, 15, 0, 0, 0, 
        121, 0, 0, 0, 21, 0, 0, 
        0, 1, 0, 0, 0, 7, 0, 0, 
        0, 1, 0, 3, 0, 48, 48, 48, 
        55, 46, 48, 48, 98, 46, 48, 48, 
        48, 48, 48, 50, 97, 50, 0, 0, 
        0, 94, 0, 0, 0, 5, 0, 4, 
        0, 123, 10, 32, 34, 99, 111, 108, 
        117, 109, 110, 115, 34, 58, 32, 91, 
        10, 123, 10, 34, 99, 111, 108, 117, 
        109, 110, 73, 100, 34, 58, 32, 49, 
        44, 10, 34, 99, 111, 108, 117, 109, 
        110, 78, 97, 109, 101, 34, 58, 32, 
        34, 73, 68, 34, 44, 10, 34, 99, 
        111, 108, 117, 109, 110, 84, 121, 112, 
        101, 34, 58, 32, 34, 78, 85, 77, 
        66, 69, 82, 34, 44, 10, 34, 99, 
        111, 108, 117, 109, 110, 80, 114, 101, 
        99, 105, 115, 105, 111, 110, 34, 58, 
        32, 54, 44, 10, 34, 99, 111, 108, 
        117, 109, 110, 83, 99, 97, 108, 101, 
        34, 58, 32, 48, 44, 10, 34, 105, 
        115, 78, 117, 108, 108, 97, 98, 108, 
        101, 34, 58, 32, 116, 114, 117, 101, 
        10, 125, 44, 10, 123, 10, 34, 99, 
        111, 108, 117, 109, 110, 73, 100, 34, 
        58, 32, 50, 44, 10, 34, 99, 111, 
        108, 117, 109, 110, 78, 97, 109, 101, 
        34, 58, 32, 34, 84, 69, 83, 84, 
        95, 78, 65, 77, 69, 34, 44, 10, 
        34, 99, 111, 108, 117, 109, 110, 84, 
        121, 112, 101, 34, 58, 32, 34, 86, 
        65, 82, 67, 72, 65, 82, 50, 34, 
        44, 10, 34, 99, 111, 108, 117, 109, 
        110, 80, 114, 101, 99, 105, 115, 105, 
        111, 110, 34, 58, 32, 45, 49, 44, 
        10, 34, 99, 111, 108, 117, 109, 110, 
        83, 99, 97, 108, 101, 34, 58, 32, 
        45, 49, 44, 10, 34, 105, 115, 78, 
        117, 108, 108, 97, 98, 108, 101, 34, 
        58, 32, 116, 114, 117, 101, 10, 125, 
        10, 93, 44, 10, 32, 34, 111, 98, 
        106, 101, 99, 116, 73, 100, 34, 58, 
        32, 50, 48, 50, 53, 51, 44, 10, 
        34, 118, 97, 108, 105, 100, 83, 105, 
        110, 99, 101, 83, 67, 78, 34, 58, 
        32, 55, 53, 52, 49, 48, 55, 44, 
        10, 34, 115, 99, 104, 101, 109, 97, 
        78, 97, 109, 101, 34, 58, 32, 34, 
        83, 79, 69, 34, 44, 10, 34, 116, 
        97, 98, 108, 101, 78, 97, 109, 101, 
        34, 58, 32, 34, 85, 78, 73, 84, 
        84, 69, 83, 84, 34, 10, 125, 0, 
        0, 3, 0, 0, 0, 32, 0, 2, 
        0, 29, 79, 0, 0, 4, 0, 0, 
        0, 2, 0, 3, 0, 47, -126, 11, 
        0, 0, 0, 0, 0, 6, 0, 0, 
        0, 50, 0, 2, 0, 21, 0, 0, 
        0, 6, 0, 0, 0, -76, 0, 0, 
        0, 1, 0, 0, 0, 4, 0, 0, 
        0, 48, 0, 2, 0, -121, -106, -104, 
        0, 0, 0, 0, 0, 68, 0, 0, 
        0, 11, 0, 0, 0, 0, 2, 0, 
        0, 7, 0, 0, 0, 1, 0, 3, 
        0, 48, 48, 48, 55, 46, 48, 48, 
        98, 46, 48, 48, 48, 48, 48, 50, 
        97, 50, 0, 0, 0, 3, 0, 0, 
        0, 32, 0, 2, 0, 29, 79, 0, 
        0, 3, 0, 0, 0, 34, 0, 2, 
        0, 83, 79, 69, 0, 5, 0, 0, 
        0, 33, 0, 2, 0, 85, 78, 73, 
        84, 84, 69, 83, 84, 0, 0, 0, 
        0, 4, 0, 0, 0, 2, 0, 3, 
        0, 47, -126, 11, 0, 0, 0, 0, 
        0, 3, 0, 0, 0, 16, 0, 2, 
        0, 1, 0, 0, 0, 3, 0, 0, 
        0, 17, 0, 2, 0, 73, 68, 0, 
        0, 4, 0, 0, 0, 18, 0, 2, 
        0, 78, 85, 77, 66, 69, 82, 0, 
        0, 6, 0, 0, 0, 50, 0, 2, 
        0, 21, 0, 0, 0, 6, 0, 0, 
        0, -76, 0, 0, 0, 1, 0, 0, 
        0, 4, 0, 0, 0, 48, 0, 2, 
        0, -124, -106, -104, 0, 0, 0, 0, 
        0, 3, 0, 0, 0, 16, 0, 2, 
        0, 2, 0, 0, 0, 5, 0, 0, 
        0, 17, 0, 2, 0, 84, 69, 83, 
        84, 95, 78, 65, 77, 69, 0, 0, 
        0, 5, 0, 0, 0, 18, 0, 2, 
        0, 86, 65, 82, 67, 72, 65, 82, 
        50, 0, 0, 0, 0, 6, 0, 0, 
        0, 50, 0, 2, 0, 21, 0, 0, 
        0, 6, 0, 0, 0, -76, 0, 0, 
        0, 1, 0, 0, 0, 4, 0, 0, 
        0, 48, 0, 2, 0, -120, -106, -104, 
        0, 0, 0, 0, 0, 56, 0, 0, 
        0, 11, 0, 0, 0, 2, 0, 0, 
        0, 7, 0, 0, 0, 1, 0, 3, 
        0, 48, 48, 48, 55, 46, 48, 48, 
        98, 46, 48, 48, 48, 48, 48, 50, 
        97, 50, 0, 0, 0, 3, 0, 0, 
        0, 32, 0, 2, 0, 29, 79, 0, 
        0, 4, 0, 0, 0, 48, 0, 2, 
        0, -117, -106, -104, 0, 0, 0, 0, 
        0, 6, 0, 0, 0, 50, 0, 2, 
        0, 21, 0, 0, 0, 5, 0, 0, 
        0, -36, 0, 0, 0, 3, 0, 0, 
        0, 4, 0, 0, 0, 2, 0, 3, 
        0, 46, -126, 11, 0, 0, 0, 0, 
        0, 3, 0, 0, 0, 3, 0, 3, 
        0, -127, -39, -81, 54, 3, 0, 0, 
        0, 16, 0, 2, 0, 1, 0, 0, 
        0, 4, 0, 0, 0, 2, 0, 2, 
        0, 2, 0, 0, 0, -63, 2, 0, 
        0, 3, 0, 0, 0, 16, 0, 2, 
        0, 2, 0, 0, 0, 6, 0, 0, 
        0, 2, 0, 2, 0, 11, 0, 0, 
        0, 84, 69, 83, 84, 32, 73, 78, 
        83, 69, 82, 84, 0, 3, 0, 0, 
        0, 1, 1, 32, 0, 0, 0, 0, 
        0, 4, 0, 0, 0, 2, 1, 32, 
        0, 4, 0, 0, 0, 1, 16, 2, 
        16, 3, 0, 0, 0, 3, 1, 32, 
        0, 0, 0, 0, 0, 14, 0, 0, 
        0, 0, 0, 0, 0, 1, 0, 0, 
        0, 3, 0, 0, 0, 4, 0, 0, 
        0, 1, 0, 0, 0, 3, 0, 0, 
        0, 2, 0, 0, 0, 80, -65, -11, 
        -81, 5, 0, 0, 0, 5, 0, 0, 
        0, 80, 76, 79, 71, 32, 69, 78, 
        68, 0, 0, 0, 0
    };
    
    private byte[] updateDataPLOGByteArray = new byte [] {
        80, 76, 79, 71, 10, 32, 13, 32, 
        1, 0, 0, 0, 1, 0, 0, 0, 
        24, 0, 0, 0, 0, 0, 0, 0, 
        0, 0, 0, 0, 3, 0, 0, 0, 
        1, 0, 1, 0, 76, -34, 96, -90, 
        12, 0, 0, 0, 6, 0, 0, 0, 
        51, 70, 56, 50, 56, 51, 53, 52, 
        45, 53, 49, 70, 65, 45, 49, 49, 
        69, 54, 45, 65, 50, 70, 67, 45, 
        54, 50, 66, 66, 69, 70, 56, 65, 
        48, 55, 67, 55, 0, 0, 0, 0, 
        3, 0, 0, 0, 7, 0, 0, 0, 
        11, 0, 0, 0, 3, 0, 0, 0, 
        8, 0, 0, 0, 15, 0, 0, 0, 
        121, 0, 0, 0, 21, 0, 0, 
        0, 1, 0, 0, 0, 7, 0, 0, 
        0, 1, 0, 3, 0, 48, 48, 48, 
        51, 46, 48, 49, 53, 46, 48, 48, 
        48, 48, 48, 51, 98, 98, 0, 0, 
        0, 94, 0, 0, 0, 5, 0, 4, 
        0, 123, 10, 32, 34, 99, 111, 108, 
        117, 109, 110, 115, 34, 58, 32, 91, 
        10, 123, 10, 34, 99, 111, 108, 117, 
        109, 110, 73, 100, 34, 58, 32, 49, 
        44, 10, 34, 99, 111, 108, 117, 109, 
        110, 78, 97, 109, 101, 34, 58, 32, 
        34, 73, 68, 34, 44, 10, 34, 99, 
        111, 108, 117, 109, 110, 84, 121, 112, 
        101, 34, 58, 32, 34, 78, 85, 77, 
        66, 69, 82, 34, 44, 10, 34, 99, 
        111, 108, 117, 109, 110, 80, 114, 101, 
        99, 105, 115, 105, 111, 110, 34, 58, 
        32, 54, 44, 10, 34, 99, 111, 108, 
        117, 109, 110, 83, 99, 97, 108, 101, 
        34, 58, 32, 48, 44, 10, 34, 105, 
        115, 78, 117, 108, 108, 97, 98, 108, 
        101, 34, 58, 32, 116, 114, 117, 101, 
        10, 125, 44, 10, 123, 10, 34, 99, 
        111, 108, 117, 109, 110, 73, 100, 34, 
        58, 32, 50, 44, 10, 34, 99, 111, 
        108, 117, 109, 110, 78, 97, 109, 101, 
        34, 58, 32, 34, 84, 69, 83, 84, 
        95, 78, 65, 77, 69, 34, 44, 10, 
        34, 99, 111, 108, 117, 109, 110, 84, 
        121, 112, 101, 34, 58, 32, 34, 86, 
        65, 82, 67, 72, 65, 82, 50, 34, 
        44, 10, 34, 99, 111, 108, 117, 109, 
        110, 80, 114, 101, 99, 105, 115, 105, 
        111, 110, 34, 58, 32, 45, 49, 44, 
        10, 34, 99, 111, 108, 117, 109, 110, 
        83, 99, 97, 108, 101, 34, 58, 32, 
        45, 49, 44, 10, 34, 105, 115, 78, 
        117, 108, 108, 97, 98, 108, 101, 34, 
        58, 32, 116, 114, 117, 101, 10, 125, 
        10, 93, 44, 10, 32, 34, 111, 98, 
        106, 101, 99, 116, 73, 100, 34, 58, 
        32, 50, 48, 50, 53, 51, 44, 10, 
        34, 118, 97, 108, 105, 100, 83, 105, 
        110, 99, 101, 83, 67, 78, 34, 58, 
        32, 55, 53, 52, 49, 48, 55, 44, 
        10, 34, 115, 99, 104, 101, 109, 97, 
        78, 97, 109, 101, 34, 58, 32, 34, 
        83, 79, 69, 34, 44, 10, 34, 116, 
        97, 98, 108, 101, 78, 97, 109, 101, 
        34, 58, 32, 34, 85, 78, 73, 84, 
        84, 69, 83, 84, 34, 10, 125, 0, 
        0, 3, 0, 0, 0, 32, 0, 2, 
        0, 29, 79, 0, 0, 4, 0, 0, 
        0, 2, 0, 3, 0, -77, -126, 11, 
        0, 0, 0, 0, 0, 6, 0, 0, 
        0, 50, 0, 2, 0, 22, 0, 0, 
        0, 3, 0, 0, 0, 76, 0, 0, 
        0, 1, 0, 0, 0, 4, 0, 0, 
        0, 48, 0, 2, 0, -124, -106, -104, 
        0, 0, 0, 0, 0, 68, 0, 0, 
        0, 11, 0, 0, 0, 0, 2, 0, 
        0, 7, 0, 0, 0, 1, 0, 3, 
        0, 48, 48, 48, 51, 46, 48, 49, 
        53, 46, 48, 48, 48, 48, 48, 51, 
        98, 98, 0, 0, 0, 3, 0, 0, 
        0, 32, 0, 2, 0, 29, 79, 0, 
        0, 3, 0, 0, 0, 34, 0, 2, 
        0, 83, 79, 69, 0, 5, 0, 0, 
        0, 33, 0, 2, 0, 85, 78, 73, 
        84, 84, 69, 83, 84, 0, 0, 0, 
        0, 4, 0, 0, 0, 2, 0, 3, 
        0, -77, -126, 11, 0, 0, 0, 0, 
        0, 3, 0, 0, 0, 16, 0, 2, 
        0, 1, 0, 0, 0, 3, 0, 0, 
        0, 17, 0, 2, 0, 73, 68, 0, 
        0, 4, 0, 0, 0, 18, 0, 2, 
        0, 78, 85, 77, 66, 69, 82, 0, 
        0, 6, 0, 0, 0, 50, 0, 2, 
        0, 22, 0, 0, 0, 3, 0, 0, 
        0, 76, 0, 0, 0, 1, 0, 0, 
        0, 4, 0, 0, 0, 48, 0, 2, 
        0, -127, -106, -104, 0, 0, 0, 0, 
        0, 3, 0, 0, 0, 16, 0, 2, 
        0, 2, 0, 0, 0, 5, 0, 0, 
        0, 17, 0, 2, 0, 84, 69, 83, 
        84, 95, 78, 65, 77, 69, 0, 0, 
        0, 5, 0, 0, 0, 18, 0, 2, 
        0, 86, 65, 82, 67, 72, 65, 82, 
        50, 0, 0, 0, 0, 6, 0, 0, 
        0, 50, 0, 2, 0, 22, 0, 0, 
        0, 3, 0, 0, 0, 76, 0, 0, 
        0, 1, 0, 0, 0, 4, 0, 0, 
        0, 48, 0, 2, 0, -123, -106, -104, 
        0, 0, 0, 0, 0, 85, 0, 0, 
        0, 11, 0, 0, 0, 5, 0, 0, 
        0, 7, 0, 0, 0, 1, 0, 3, 
        0, 48, 48, 48, 51, 46, 48, 49, 
        53, 46, 48, 48, 48, 48, 48, 51, 
        98, 98, 0, 0, 0, 3, 0, 0, 
        0, 32, 0, 2, 0, 29, 79, 0, 
        0, 4, 0, 0, 0, 48, 0, 2, 
        0, -120, -106, -104, 0, 0, 0, 0, 
        0, 6, 0, 0, 0, 50, 0, 2, 
        0, 22, 0, 0, 0, 2, 0, 0, 
        0, 0, 0, 0, 0, 3, 0, 0, 
        0, 3, 0, 0, 0, 16, 0, 2, 
        0, 2, 0, 0, 0, 6, 0, 0, 
        0, 3, 0, 2, 0, 11, 0, 0, 
        0, 84, 69, 83, 84, 32, 73, 78, 
        83, 69, 82, 84, 0, 3, 0, 0, 
        0, 1, 1, 32, 0, 0, 0, 0, 
        0, 3, 0, 0, 0, 2, 1, 32, 
        0, 0, 0, 0, 0, 4, 0, 0, 
        0, 3, 1, 32, 0, 2, 0, 0, 
        0, 2, 16, 0, 0, 3, 0, 0, 
        0, 16, 0, 2, 0, 1, 0, 0, 
        0, 4, 0, 0, 0, 3, 0, 2, 
        0, 2, 0, 0, 0, -63, 2, 0, 
        0, 3, 0, 0, 0, 1, 1, 32, 
        0, 0, 0, 0, 0, 3, 0, 0, 
        0, 2, 1, 32, 0, 0, 0, 0, 
        0, 4, 0, 0, 0, 3, 1, 32, 
        0, 2, 0, 0, 0, 1, 16, 0, 
        0, 4, 0, 0, 0, 2, 0, 3, 
        0, -78, -126, 11, 0, 0, 0, 0, 
        0, 3, 0, 0, 0, 3, 0, 3, 
        0, -124, -39, -81, 54, 3, 0, 0, 
        0, 16, 0, 2, 0, 2, 0, 0, 
        0, 6, 0, 0, 0, 2, 0, 2, 
        0, 11, 0, 0, 0, 84, 69, 83, 
        84, 32, 85, 80, 68, 65, 84, 69, 
        0, 3, 0, 0, 0, 1, 1, 32, 
        0, 0, 0, 0, 0, 4, 0, 0, 
        0, 2, 1, 32, 0, 2, 0, 0, 
        0, 2, 16, 0, 0, 3, 0, 0, 
        0, 3, 1, 32, 0, 0, 0, 0, 
        0, 14, 0, 0, 0, 0, 0, 0, 
        0, 1, 0, 0, 0, 3, 0, 0, 
        0, 4, 0, 0, 0, 1, 0, 0, 
        0, 3, 0, 0, 0, 2, 0, 0, 
        0, 72, -72, 100, -38, 5, 0, 0, 
        0, 5, 0, 0, 0, 80, 76, 79, 
        71, 32, 69, 78, 68, 0, 0, 0, 
        0
    };
    
    /**
     * raw PLOG byte array for
     * 
     * DELETE FROM SOE.UNITTEST WHERE ID = 1;
     */
    private byte [] deleteDataPLOGByteArray = new byte [] {
        80, 76, 79, 71, 10, 32, 13, 32, 
        1, 0, 0, 0, 1, 0, 0, 0, 
        24, 0, 0, 0, 0, 0, 0, 0, 
        0, 0, 0, 0, 3, 0, 0, 0, 
        1, 0, 1, 0, 76, -34, 96, -90, 
        12, 0, 0, 0, 6, 0, 0, 0, 
        51, 70, 56, 50, 56, 51, 53, 52, 
        45, 53, 49, 70, 65, 45, 49, 49, 
        69, 54, 45, 65, 50, 70, 67, 45, 
        54, 50, 66, 66, 69, 70, 56, 65, 
        48, 55, 67, 55, 0, 0, 0, 0, 
        3, 0, 0, 0, 7, 0, 0, 0, 
        11, 0, 0, 0, 3, 0, 0, 0, 
        8, 0, 0, 0, 15, 0, 0, 0, 
        121, 0, 0, 0, 21, 0, 0, 
        0, 1, 0, 0, 0, 7, 0, 0, 
        0, 1, 0, 3, 0, 48, 48, 48, 
        53, 46, 48, 48, 55, 46, 48, 48, 
        48, 48, 48, 51, 57, 53, 0, 0, 
        0, 94, 0, 0, 0, 5, 0, 4, 
        0, 123, 10, 32, 34, 99, 111, 108, 
        117, 109, 110, 115, 34, 58, 32, 91, 
        10, 123, 10, 34, 99, 111, 108, 117, 
        109, 110, 73, 100, 34, 58, 32, 49, 
        44, 10, 34, 99, 111, 108, 117, 109, 
        110, 78, 97, 109, 101, 34, 58, 32, 
        34, 73, 68, 34, 44, 10, 34, 99, 
        111, 108, 117, 109, 110, 84, 121, 112, 
        101, 34, 58, 32, 34, 78, 85, 77, 
        66, 69, 82, 34, 44, 10, 34, 99, 
        111, 108, 117, 109, 110, 80, 114, 101, 
        99, 105, 115, 105, 111, 110, 34, 58, 
        32, 54, 44, 10, 34, 99, 111, 108, 
        117, 109, 110, 83, 99, 97, 108, 101, 
        34, 58, 32, 48, 44, 10, 34, 105, 
        115, 78, 117, 108, 108, 97, 98, 108, 
        101, 34, 58, 32, 116, 114, 117, 101, 
        10, 125, 44, 10, 123, 10, 34, 99, 
        111, 108, 117, 109, 110, 73, 100, 34, 
        58, 32, 50, 44, 10, 34, 99, 111, 
        108, 117, 109, 110, 78, 97, 109, 101, 
        34, 58, 32, 34, 84, 69, 83, 84, 
        95, 78, 65, 77, 69, 34, 44, 10, 
        34, 99, 111, 108, 117, 109, 110, 84, 
        121, 112, 101, 34, 58, 32, 34, 86, 
        65, 82, 67, 72, 65, 82, 50, 34, 
        44, 10, 34, 99, 111, 108, 117, 109, 
        110, 80, 114, 101, 99, 105, 115, 105, 
        111, 110, 34, 58, 32, 45, 49, 44, 
        10, 34, 99, 111, 108, 117, 109, 110, 
        83, 99, 97, 108, 101, 34, 58, 32, 
        45, 49, 44, 10, 34, 105, 115, 78, 
        117, 108, 108, 97, 98, 108, 101, 34, 
        58, 32, 116, 114, 117, 101, 10, 125, 
        10, 93, 44, 10, 32, 34, 111, 98, 
        106, 101, 99, 116, 73, 100, 34, 58, 
        32, 50, 48, 50, 53, 51, 44, 10, 
        34, 118, 97, 108, 105, 100, 83, 105, 
        110, 99, 101, 83, 67, 78, 34, 58, 
        32, 55, 53, 52, 49, 48, 55, 44, 
        10, 34, 115, 99, 104, 101, 109, 97, 
        78, 97, 109, 101, 34, 58, 32, 34, 
        83, 79, 69, 34, 44, 10, 34, 116, 
        97, 98, 108, 101, 78, 97, 109, 101, 
        34, 58, 32, 34, 85, 78, 73, 84, 
        84, 69, 83, 84, 34, 10, 125, 0, 
        0, 3, 0, 0, 0, 32, 0, 2, 
        0, 29, 79, 0, 0, 4, 0, 0, 
        0, 2, 0, 3, 0, 61, -125, 11, 
        0, 0, 0, 0, 0, 6, 0, 0, 
        0, 50, 0, 2, 0, 23, 0, 0, 
        0, 3, 0, 0, 0, 68, 0, 0, 
        0, 1, 0, 0, 0, 4, 0, 0, 
        0, 48, 0, 2, 0, -124, -106, -104, 
        0, 0, 0, 0, 0, 68, 0, 0, 
        0, 11, 0, 0, 0, 0, 2, 0, 
        0, 7, 0, 0, 0, 1, 0, 3, 
        0, 48, 48, 48, 53, 46, 48, 48, 
        55, 46, 48, 48, 48, 48, 48, 51, 
        57, 53, 0, 0, 0, 3, 0, 0, 
        0, 32, 0, 2, 0, 29, 79, 0, 
        0, 3, 0, 0, 0, 34, 0, 2, 
        0, 83, 79, 69, 0, 5, 0, 0, 
        0, 33, 0, 2, 0, 85, 78, 73, 
        84, 84, 69, 83, 84, 0, 0, 0, 
        0, 4, 0, 0, 0, 2, 0, 3, 
        0, 61, -125, 11, 0, 0, 0, 0, 
        0, 3, 0, 0, 0, 16, 0, 2, 
        0, 1, 0, 0, 0, 3, 0, 0, 
        0, 17, 0, 2, 0, 73, 68, 0, 
        0, 4, 0, 0, 0, 18, 0, 2, 
        0, 78, 85, 77, 66, 69, 82, 0, 
        0, 6, 0, 0, 0, 50, 0, 2, 
        0, 23, 0, 0, 0, 3, 0, 0, 
        0, 68, 0, 0, 0, 1, 0, 0, 
        0, 4, 0, 0, 0, 48, 0, 2, 
        0, -127, -106, -104, 0, 0, 0, 0, 
        0, 3, 0, 0, 0, 16, 0, 2, 
        0, 2, 0, 0, 0, 5, 0, 0, 
        0, 17, 0, 2, 0, 84, 69, 83, 
        84, 95, 78, 65, 77, 69, 0, 0, 
        0, 5, 0, 0, 0, 18, 0, 2, 
        0, 86, 65, 82, 67, 72, 65, 82, 
        50, 0, 0, 0, 0, 6, 0, 0, 
        0, 50, 0, 2, 0, 23, 0, 0, 
        0, 3, 0, 0, 0, 68, 0, 0, 
        0, 1, 0, 0, 0, 4, 0, 0, 
        0, 48, 0, 2, 0, -123, -106, -104, 
        0, 0, 0, 0, 0, 56, 0, 0, 
        0, 11, 0, 0, 0, 3, 0, 0, 
        0, 7, 0, 0, 0, 1, 0, 3, 
        0, 48, 48, 48, 53, 46, 48, 48, 
        55, 46, 48, 48, 48, 48, 48, 51, 
        57, 53, 0, 0, 0, 3, 0, 0, 
        0, 32, 0, 2, 0, 29, 79, 0, 
        0, 4, 0, 0, 0, 48, 0, 2, 
        0, -120, -106, -104, 0, 0, 0, 0, 
        0, 6, 0, 0, 0, 50, 0, 2, 
        0, 23, 0, 0, 0, 2, 0, 0, 
        0, 0, 0, 0, 0, 3, 0, 0, 
        0, 3, 0, 0, 0, 16, 0, 2, 
        0, 1, 0, 0, 0, 4, 0, 0, 
        0, 3, 0, 2, 0, 2, 0, 0, 
        0, -63, 2, 0, 0, 3, 0, 0, 
        0, 16, 0, 2, 0, 2, 0, 0, 
        0, 6, 0, 0, 0, 3, 0, 2, 
        0, 11, 0, 0, 0, 84, 69, 83, 
        84, 32, 85, 80, 68, 65, 84, 69, 
        0, 3, 0, 0, 0, 1, 1, 32, 
        0, 0, 0, 0, 0, 3, 0, 0, 
        0, 2, 1, 32, 0, 0, 0, 0, 
        0, 4, 0, 0, 0, 3, 1, 32, 
        0, 4, 0, 0, 0, 1, 16, 2, 
        16, 4, 0, 0, 0, 2, 0, 3, 
        0, 60, -125, 11, 0, 0, 0, 0, 
        0, 3, 0, 0, 0, 3, 0, 3, 
        0, -121, -39, -81, 54, 14, 0, 0, 
        0, 0, 0, 0, 0, 1, 0, 0, 
        0, 3, 0, 0, 0, 4, 0, 0, 
        0, 1, 0, 0, 0, 3, 0, 0, 
        0, 2, 0, 0, 0, 126, -56, -121, 
        -12, 5, 0, 0, 0, 5, 0, 0, 
        0, 80, 76, 79, 71, 32, 69, 78, 
        68, 0, 0, 0, 0
    };
    
    /**
     * PLOG data byte array referencing included/child PLOGs
     */
    private byte[] includePLOGDataByteArray = new byte[] {
        80, 76, 79, 71, 10, 32, 13, 32, 
        1, 0, 0, 0, 1, 0, 0, 0, 
        24, 0, 0, 0, 0, 0, 0, 0, 
        0, 0, 0, 0, 3, 0, 0, 0, 
        1, 0, 1, 0, 0, 0, 0, 0, 
        12, 0, 0, 0, 6, 0, 0, 0, 
        48, 66, 48, 57, 52, 49, 51, 56, 
        45, 55, 57, 50, 53, 45, 49, 49, 
        69, 54, 45, 56, 50, 65, 53, 45, 
        69, 56, 55, 57, 65, 52, 65, 51, 
        50, 51, 54, 68, 0, 0, 0, 0, 
        3, 0, 0, 0, 7, 0, 0, 0, 
        -20, 1, 0, 0, 3, 0, 0, 0, 
        8, 0, 0, 0, 15, 0, 0, 0, 
        66, 0, 0, 0, 20, 0, 0, 0, 
        1, 0, 0, 0, 12, 0, 0, 0, 
        6, 0, 0, 0, 48, 66, 48, 57, 
        52, 49, 51, 56, 45, 55, 57, 50, 
        53, 45, 49, 49, 69, 54, 45, 56, 
        50, 65, 53, 45, 69, 56, 55, 57, 
        65, 52, 65, 51, 50, 51, 54, 68, 
        0, 0, 0, 0, 4, 0, 0, 0, 
        2, 0, 3, 0, -54, 103, -16, 0, 
        0, 0, 0, 0, 11, 0, 0, 0, 
        16, 0, 0, 0, 76, 79, 65, 68, 
        32, 116, 97, 98, 108, 101, 32, 83, 
        67, 79, 84, 84, 46, 84, 69, 83, 
        84, 49, 32, 97, 116, 32, 49, 53, 
        55, 53, 53, 50, 49, 48, 0, 0, 
        16, 0, 0, 0, 17, 0, 0, 0, 
        52, 57, 50, 46, 112, 108, 111, 103, 
        46, 49, 52, 55, 51, 55, 49, 49, 
        49, 51, 49, 45, 48, 48, 48, 48, 
        48, 49, 45, 76, 79, 65, 68, 95, 
        50, 57, 53, 57, 50, 45, 83, 67, 
        79, 84, 84, 46, 84, 69, 83, 84, 
        49, 45, 65, 80, 80, 76, 89, 0, 
        3, 0, 0, 0, 7, 0, 0, 0, 
        1, 0, 0, 0, 6, 0, 0, 0, 
        50, 0, 2, 0, -20, 1, 0, 0, 
        -69, 28, 0, 0, -128, 1, 0, 0, 
        1, 0, 0, 0, 4, 0, 0, 0, 
        18, 0, 0, 0, 65, 80, 80, 76, 
        89, 0, 0, 0, 4, 0, 0, 0, 
        2, 0, 3, 0, -54, 103, -16, 0, 
        0, 0, 0, 0, 3, 0, 0, 0, 
        3, 0, 3, 0, 90, -104, -6, 54, 
        80, 0, 0, 0, 20, 0, 0, 0, 
        2, 0, 0, 0, 12, 0, 0, 0, 
        6, 0, 0, 0, 48, 66, 48, 57, 
        52, 49, 51, 56, 45, 55, 57, 50, 
        53, 45, 49, 49, 69, 54, 45, 56, 
        50, 65, 53, 45, 69, 56, 55, 57, 
        65, 52, 65, 51, 50, 51, 54, 68, 
        0, 0, 0, 0, 4, 0, 0, 0, 
        2, 0, 3, 0, -54, 103, -16, 0, 
        0, 0, 0, 0, 11, 0, 0, 0, 
        16, 0, 0, 0, 76, 79, 65, 68, 
        32, 116, 97, 98, 108, 101, 32, 83, 
        67, 79, 84, 84, 46, 84, 69, 83, 
        84, 49, 32, 97, 116, 32, 49, 53, 
        55, 53, 53, 50, 49, 48, 0, 0, 
        16, 0, 0, 0, 17, 0, 0, 0, 
        52, 57, 50, 46, 112, 108, 111, 103, 
        46, 49, 52, 55, 51, 55, 49, 49, 
        49, 51, 49, 45, 48, 48, 48, 48, 
        48, 49, 45, 76, 79, 65, 68, 95, 
        50, 57, 53, 57, 50, 45, 83, 67, 
        79, 84, 84, 46, 84, 69, 83, 84, 
        49, 45, 65, 80, 80, 76, 89, 0, 
        3, 0, 0, 0, 7, 0, 0, 0, 
        1, 0, 0, 0, 6, 0, 0, 0, 
        50, 0, 2, 0, -20, 1, 0, 0, 
        -69, 28, 0, 0, -128, 1, 0, 0, 
        2, 0, 0, 0, 4, 0, 0, 0, 
        18, 0, 0, 0, 65, 80, 80, 76, 
        89, 0, 0, 0, 3, 0, 0, 0, 
        19, 0, 0, 0, 18, 0, 0, 0, 
        3, 0, 0, 0, 32, 0, 2, 0, 
        -104, 115, 0, 0, 4, 0, 0, 0, 
        34, 0, 2, 0, 83, 67, 79, 84, 
        84, 0, 0, 0, 4, 0, 0, 0, 
        33, 0, 2, 0, 84, 69, 83, 84, 
        49, 0, 0, 0, 4, 0, 0, 0, 
        2, 0, 3, 0, -54, 103, -16, 0, 
        0, 0, 0, 0, 3, 0, 0, 0, 
        3, 0, 3, 0, 90, -104, -6, 54, 
        66, 0, 0, 0, 20, 0, 0, 0, 
        1, 0, 0, 0, 12, 0, 0, 0, 
        6, 0, 0, 0, 48, 66, 48, 57, 
        52, 49, 51, 56, 45, 55, 57, 50, 
        53, 45, 49, 49, 69, 54, 45, 56, 
        50, 65, 53, 45, 69, 56, 55, 57, 
        65, 52, 65, 51, 50, 51, 54, 68, 
        0, 0, 0, 0, 4, 0, 0, 0, 
        2, 0, 3, 0, -54, 103, -16, 0, 
        0, 0, 0, 0, 11, 0, 0, 0, 
        16, 0, 0, 0, 76, 79, 65, 68, 
        32, 116, 97, 98, 108, 101, 32, 83, 
        67, 79, 84, 84, 46, 84, 69, 83, 
        84, 50, 32, 97, 116, 32, 49, 53, 
        55, 53, 53, 50, 49, 48, 0, 0, 
        16, 0, 0, 0, 17, 0, 0, 0, 
        52, 57, 50, 46, 112, 108, 111, 103, 
        46, 49, 52, 55, 51, 55, 49, 49, 
        49, 51, 49, 45, 48, 48, 48, 48, 
        48, 50, 45, 76, 79, 65, 68, 95, 
        50, 57, 53, 57, 51, 45, 83, 67, 
        79, 84, 84, 46, 84, 69, 83, 84, 
        50, 45, 65, 80, 80, 76, 89, 0, 
        3, 0, 0, 0, 7, 0, 0, 0, 
        2, 0, 0, 0, 6, 0, 0, 0, 
        50, 0, 2, 0, -20, 1, 0, 0, 
        -69, 28, 0, 0, -128, 1, 0, 0, 
        3, 0, 0, 0, 4, 0, 0, 0, 
        18, 0, 0, 0, 65, 80, 80, 76, 
        89, 0, 0, 0, 4, 0, 0, 0, 
        2, 0, 3, 0, -54, 103, -16, 0, 
        0, 0, 0, 0, 3, 0, 0, 0, 
        3, 0, 3, 0, 90, -104, -6, 54, 
        80, 0, 0, 0, 20, 0, 0, 0, 
        2, 0, 0, 0, 12, 0, 0, 0, 
        6, 0, 0, 0, 48, 66, 48, 57, 
        52, 49, 51, 56, 45, 55, 57, 50, 
        53, 45, 49, 49, 69, 54, 45, 56, 
        50, 65, 53, 45, 69, 56, 55, 57, 
        65, 52, 65, 51, 50, 51, 54, 68, 
        0, 0, 0, 0, 4, 0, 0, 0, 
        2, 0, 3, 0, -54, 103, -16, 0, 
        0, 0, 0, 0, 11, 0, 0, 0, 
        16, 0, 0, 0, 76, 79, 65, 68, 
        32, 116, 97, 98, 108, 101, 32, 83, 
        67, 79, 84, 84, 46, 84, 69, 83, 
        84, 50, 32, 97, 116, 32, 49, 53, 
        55, 53, 53, 50, 49, 48, 0, 0, 
        16, 0, 0, 0, 17, 0, 0, 0, 
        52, 57, 50, 46, 112, 108, 111, 103, 
        46, 49, 52, 55, 51, 55, 49, 49, 
        49, 51, 49, 45, 48, 48, 48, 48, 
        48, 50, 45, 76, 79, 65, 68, 95, 
        50, 57, 53, 57, 51, 45, 83, 67, 
        79, 84, 84, 46, 84, 69, 83, 84, 
        50, 45, 65, 80, 80, 76, 89, 0, 
        3, 0, 0, 0, 7, 0, 0, 0, 
        2, 0, 0, 0, 6, 0, 0, 0, 
        50, 0, 2, 0, -20, 1, 0, 0, 
        -69, 28, 0, 0, -128, 1, 0, 0, 
        4, 0, 0, 0, 4, 0, 0, 0, 
        18, 0, 0, 0, 65, 80, 80, 76, 
        89, 0, 0, 0, 3, 0, 0, 0, 
        19, 0, 0, 0, 18, 0, 0, 0, 
        3, 0, 0, 0, 32, 0, 2, 0, 
        -103, 115, 0, 0, 4, 0, 0, 0, 
        34, 0, 2, 0, 83, 67, 79, 84, 
        84, 0, 0, 0, 4, 0, 0, 0, 
        33, 0, 2, 0, 84, 69, 83, 84, 
        50, 0, 0, 0, 4, 0, 0, 0, 
        2, 0, 3, 0, -54, 103, -16, 0, 
        0, 0, 0, 0, 3, 0, 0, 0, 
        3, 0, 3, 0, 90, -104, -6, 54, 
        14, 0, 0, 0, 0, 0, 0, 0, 
        1, 0, 0, 0, 3, 0, 0, 0, 
        4, 0, 0, 0, 1, 0, 0, 0, 
        3, 0, 0, 0, 2, 0, 0, 0, 
        81, 95, 50, -60, 5, 0, 0, 0, 
        5, 0, 0, 0, 80, 76, 79, 71, 
        32, 69, 78, 68, 0, 0, 0, 0
    };
    
}
