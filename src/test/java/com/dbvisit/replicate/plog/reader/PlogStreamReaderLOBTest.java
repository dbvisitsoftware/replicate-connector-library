package com.dbvisit.replicate.plog.reader;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.dbvisit.replicate.plog.domain.parser.DomainParser;
import com.dbvisit.replicate.plog.domain.parser.ChangeRowParser;
import com.dbvisit.replicate.plog.domain.parser.MetaDataParser;
import com.dbvisit.replicate.plog.domain.parser.ProxyDomainParser;
import com.dbvisit.replicate.plog.domain.parser.TransactionInfoParser;
import com.dbvisit.replicate.plog.file.PlogFile;
import com.dbvisit.replicate.plog.format.EntrySubType;
import com.dbvisit.replicate.plog.format.EntryType;
import com.dbvisit.replicate.plog.reader.criteria.TypeCriteria;

public class PlogStreamReaderLOBTest {
    private static final Logger logger = LoggerFactory.getLogger(
        PlogStreamReaderTest.class
    );

    /* Test reading an LCR of 3 parts, one data fields, one CLOB, one BLOB
     * LCR. The CLOB is UTF16, but defined as CLOB in Oracle. The incoming
     * NOOP LCR before CLOB claims it is CLOB_UTF16 - this is to test the
     * workaround for it in merging schema definitions and ensure that
     * it was decoded correctly. Without the workarond this will results
     * in an "Incompatible DDL" exception which is not correct seeing that
     * a NOOP introduced the column and not DDL JSON.
     */
    @Test
    public void testReadCLOB16TypeDataPLOGStream() {
        try {
            InputStream is = new ByteArrayInputStream (clob16DataByteArray);
            DataInputStream dis = new DataInputStream (is);
            
            // do not merge, emit LOBs separately
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
            
            for (DomainRecord drec : drs) {
                logger.info (drec.getRecordSchema());
                logger.info (drec.toJSONString());
            }
            
            final int COUNT = 6;
            final DomainRecordType[] TYPES = new DomainRecordType[] {
                DomainRecordType.METADATA_RECORD,
                DomainRecordType.CHANGEROW_RECORD,
                DomainRecordType.CHANGEROW_RECORD,
                DomainRecordType.CHANGEROW_RECORD,
                DomainRecordType.CHANGEROW_RECORD,
                DomainRecordType.CHANGEROW_RECORD
            };
            final String SCHEMA_NAME = "SCOTT.TEST2";
            final ChangeAction[] RECORD_ACTIONS = new ChangeAction[] {
                    ChangeAction.NO_OPERATION,
                    ChangeAction.NO_OPERATION,
                    ChangeAction.INSERT,
                    ChangeAction.LOB_WRITE,
                    ChangeAction.LOB_WRITE
            };
            final int CLOB_LCR_IDX = 4;
            final int BLOB_LCR_IDX = 5;
            final int NUM_COLUMNS = 3;
            final ColumnValue ID_RECORD = new ColumnValue (
                1,
                ColumnDataType.NUMBER,
                "ID",
                "1.00",
                false
            );
            final ColumnValue DETAILS_RECORD = new ColumnValue (
                2,
                ColumnDataType.CLOB,
                "DETAILS",
                "this is some detail that I am adding in and I am now going",
                false
            );
            final ColumnValue IMG_DETAILS_RECORD = new ColumnValue (
                3,
                ColumnDataType.BLOB,
                "IMG_DETAILS",
                "56554D564848504D43585845595143435A444C5A555A504746504C4",
                false
            );
            
            assertTrue (
                "Expecting " + COUNT + " of domain records, got: " +
                drs.size(),
                drs.size() == COUNT
            );
            
            int c = 0;
            for (int l = 0; l < COUNT; l++) {
                DomainRecord dr = drs.get(l);
                
                assertTrue (
                    "Expecting domain record of type: " + TYPES[l] + 
                    " but found: " + dr.getDomainRecordType(),
                    dr.getDomainRecordType().equals (TYPES[l])
                );
                
                logger.info (dr.toJSONString());
                
                if (dr.isChangeRowRecord()) {
                    ChangeRowRecord cr = ((ChangeRowRecord)dr);
                    assertTrue (
                        "Expecting domain record type: " + RECORD_ACTIONS[c] +
                        ", got: " + cr.getAction(),
                        cr.getAction().equals (RECORD_ACTIONS[c])
                    );
                    c++;
                }
            }
            
            /* the PLOG should contain a schema definition for this table */
            assertTrue (
                "PLOG should contain cache for schema: " + SCHEMA_NAME,
                p.getSchemas().containsKey(SCHEMA_NAME)
            );
            
            /* only evaluate LOB fields */
            ChangeRowRecord clobLCR = 
                (ChangeRowRecord)drs.get(CLOB_LCR_IDX);
           
            List <ColumnValue> crs = clobLCR.getColumnValues();
            
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
                rec1.getId() == ID_RECORD.getId() &&
                rec1.getName().equals(ID_RECORD.getName()) &&
                rec1.getType() == ID_RECORD.getType() &&
                rec1.getValueAsString().equals (ID_RECORD.getValue())
            );
            
            /* in NO-OP this would have been reported as a field of type
             * CLOB_UTF16, here we test that it stayed CLOB as originally
             * defined and that it do not result in bogus DDL exception
             */
            ColumnValue rec2 = crs.get(1);
            
            logger.info ("Record 2: " + rec2.toString().substring(0, 50));
            
            /* for legibility only use substring of value, it doesn't matter
             * because this test is to verify that data type stays
             * CLOB */
            assertTrue (
                "Expecting second column record: " + 
                DETAILS_RECORD.toString() + 
                ", found: " + rec2.toString().substring(0, 50),
                rec2.getId() == DETAILS_RECORD.getId() &&
                rec2.getName().equals(DETAILS_RECORD.getName()) &&
                rec2.getType() == DETAILS_RECORD.getType() &&
                rec2.getValueAsString()
                    .substring(
                        0,
                        DETAILS_RECORD.getValue()
                                      .toString()
                                      .length()
                    ).equals (DETAILS_RECORD.getValue())
            );
            
            ColumnValue rec3 = crs.get(2);
            
            assertTrue (
                "Expecting NULL BLOB field in first LOB LCR",
                rec3 == null
            );
            
            ChangeRowRecord blobLCR = 
                (ChangeRowRecord)drs.get(BLOB_LCR_IDX);
               
            crs = blobLCR.getColumnValues();
                
            assertTrue (
                "Expecting " + NUM_COLUMNS + " of data columns, found: " +
                crs.size(),
                crs.size() == NUM_COLUMNS
            );

            rec1 = crs.get(0);
                
            logger.info ("Record 1: " + rec1.toString());
                
            assertTrue (
                "Expecting first column record: " + ID_RECORD.toString() + 
                ", found: " + rec1.toString(),
                rec1.getId() == ID_RECORD.getId() &&
                rec1.getName().equals(ID_RECORD.getName()) &&
                rec1.getType() == ID_RECORD.getType() &&
                rec1.getValueAsString().equals (ID_RECORD.getValue())
            );
                
            rec2 = crs.get(1);
                
            assertTrue (
                "Expecting NULL CLOB field in second LOB LCR",
                rec2 == null
            );
                
            rec3 = crs.get(2);
                
            
            logger.info ("Record 3: " + rec3.toString().substring(0, 50));
            
            /*  /* for legibility only use substring of value */
            assertTrue (
                "Expecting third column record: " + 
                IMG_DETAILS_RECORD.toString() + 
                ", found: " + rec3.toString().substring(0, 50),
                rec3.getId() == IMG_DETAILS_RECORD.getId() &&
                rec3.getName().equals(IMG_DETAILS_RECORD.getName()) &&
                rec3.getType() == IMG_DETAILS_RECORD.getType() &&
                rec3.getValueAsString()
                    .substring(
                        0,
                        IMG_DETAILS_RECORD.getValue()
                                      .toString()
                                      .length()
                    ).equals (IMG_DETAILS_RECORD.getValue())
            );
            
            psr.close();
            
            
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    /**
     * INSERT for table with NUMBER, CLOB and BLOB field
     * 
     * below is the byte array for
     * <ul> 
     * <li>1 meta data</li>
     * <li>2 NOOP</li>
     * <li>1 INSERT LCR</li>
     * <li>2 LOB WRITE LCRs</li>
     * </ul>
     */
    private byte[] clob16DataByteArray = new byte[] {
        80, 76, 79, 71, 10, 32, 13, 32, 
        1, 0, 0, 0, 1, 0, 0, 0, 
        24, 0, 0, 0, 0, 0, 0, 0, 
        0, 0, 0, 0, 3, 0, 0, 0, 
        1, 0, 1, 0, 99, -58, -7, -96, 
        12, 0, 0, 0, 6, 0, 0, 0, 
        69, 65, 67, 48, 56, 49, 52, 54, 
        45, 67, 49, 66, 48, 45, 49, 49, 
        69, 54, 45, 65, 48, 56, 65, 45, 
        50, 53, 65, 55, 48, 48, 70, 66, 
        69, 53, 48, 54, 0, 0, 0, 0, 
        3, 0, 0, 0, 7, 0, 0, 0, 
        -77, 6, 0, 0, 3, 0, 0, 0, 
        8, 0, 0, 0, 15, 0, 0, 0, 
        -103, 0, 0, 0, 21, 0, 0, 0, 
        1, 0, 0, 0, 7, 0, 0, 0, 
        1, 0, 3, 0, 48, 48, 48, 50, 
        46, 48, 49, 53, 46, 48, 48, 48, 
        48, 57, 98, 54, 48, 0, 0, 0, 
        126, 0, 0, 0, 5, 0, 4, 0, 
        123, 10, 32, 34, 99, 111, 108, 117, 
        109, 110, 115, 34, 58, 32, 91, 10, 
        123, 10, 34, 99, 111, 108, 117, 109, 
        110, 73, 100, 34, 58, 32, 49, 44, 
        10, 34, 99, 111, 108, 117, 109, 110, 
        78, 97, 109, 101, 34, 58, 32, 34, 
        73, 68, 34, 44, 10, 34, 99, 111, 
        108, 117, 109, 110, 84, 121, 112, 101, 
        34, 58, 32, 34, 78, 85, 77, 66, 
        69, 82, 34, 44, 10, 34, 99, 111, 
        108, 117, 109, 110, 80, 114, 101, 99, 
        105, 115, 105, 111, 110, 34, 58, 32, 
        54, 44, 10, 34, 99, 111, 108, 117, 
        109, 110, 83, 99, 97, 108, 101, 34, 
        58, 32, 50, 44, 10, 34, 105, 115, 
        78, 117, 108, 108, 97, 98, 108, 101, 
        34, 58, 32, 116, 114, 117, 101, 10, 
        125, 44, 10, 123, 10, 34, 99, 111, 
        108, 117, 109, 110, 73, 100, 34, 58, 
        32, 50, 44, 10, 34, 99, 111, 108, 
        117, 109, 110, 78, 97, 109, 101, 34, 
        58, 32, 34, 68, 69, 84, 65, 73, 
        76, 83, 34, 44, 10, 34, 99, 111, 
        108, 117, 109, 110, 84, 121, 112, 101, 
        34, 58, 32, 34, 67, 76, 79, 66, 
        34, 44, 10, 34, 99, 111, 108, 117, 
        109, 110, 80, 114, 101, 99, 105, 115, 
        105, 111, 110, 34, 58, 32, 45, 49, 
        44, 10, 34, 99, 111, 108, 117, 109, 
        110, 83, 99, 97, 108, 101, 34, 58, 
        32, 45, 49, 44, 10, 34, 105, 115, 
        78, 117, 108, 108, 97, 98, 108, 101, 
        34, 58, 32, 116, 114, 117, 101, 10, 
        125, 44, 10, 123, 10, 34, 99, 111, 
        108, 117, 109, 110, 73, 100, 34, 58, 
        32, 51, 44, 10, 34, 99, 111, 108, 
        117, 109, 110, 78, 97, 109, 101, 34, 
        58, 32, 34, 73, 77, 71, 95, 68, 
        69, 84, 65, 73, 76, 83, 34, 44, 
        10, 34, 99, 111, 108, 117, 109, 110, 
        84, 121, 112, 101, 34, 58, 32, 34, 
        66, 76, 79, 66, 34, 44, 10, 34, 
        99, 111, 108, 117, 109, 110, 80, 114, 
        101, 99, 105, 115, 105, 111, 110, 34, 
        58, 32, 45, 49, 44, 10, 34, 99, 
        111, 108, 117, 109, 110, 83, 99, 97, 
        108, 101, 34, 58, 32, 45, 49, 44, 
        10, 34, 105, 115, 78, 117, 108, 108, 
        97, 98, 108, 101, 34, 58, 32, 116, 
        114, 117, 101, 10, 125, 10, 93, 44, 
        10, 32, 34, 111, 98, 106, 101, 99, 
        116, 73, 100, 34, 58, 32, 52, 52, 
        51, 48, 55, 44, 10, 34, 118, 97, 
        108, 105, 100, 83, 105, 110, 99, 101, 
        83, 67, 78, 34, 58, 32, 50, 57, 
        53, 57, 49, 56, 55, 56, 44, 10, 
        34, 115, 99, 104, 101, 109, 97, 78, 
        97, 109, 101, 34, 58, 32, 34, 83, 
        67, 79, 84, 84, 34, 44, 10, 34, 
        116, 97, 98, 108, 101, 78, 97, 109, 
        101, 34, 58, 32, 34, 84, 69, 83, 
        84, 50, 34, 10, 125, 0, 0, 0, 
        3, 0, 0, 0, 32, 0, 2, 0, 
        19, -83, 0, 0, 4, 0, 0, 0, 
        2, 0, 3, 0, 79, -119, -61, 1, 
        0, 0, 0, 0, 6, 0, 0, 0, 
        50, 0, 2, 0, -77, 6, 0, 0, 
        -16, 4, 0, 0, 72, 0, 0, 0, 
        2, 0, 0, 0, 4, 0, 0, 0, 
        48, 0, 2, 0, -88, -100, -104, 0, 
        0, 0, 0, 0, 88, 0, 0, 0, 
        11, 0, 0, 0, 0, 2, 0, 0, 
        7, 0, 0, 0, 1, 0, 3, 0, 
        48, 48, 48, 50, 46, 48, 49, 53, 
        46, 48, 48, 48, 48, 57, 98, 54, 
        48, 0, 0, 0, 3, 0, 0, 0, 
        32, 0, 2, 0, 19, -83, 0, 0, 
        4, 0, 0, 0, 34, 0, 2, 0, 
        83, 67, 79, 84, 84, 0, 0, 0, 
        4, 0, 0, 0, 33, 0, 2, 0, 
        84, 69, 83, 84, 50, 0, 0, 0, 
        4, 0, 0, 0, 2, 0, 3, 0, 
        79, -119, -61, 1, 0, 0, 0, 0, 
        3, 0, 0, 0, 16, 0, 2, 0, 
        1, 0, 0, 0, 3, 0, 0, 0, 
        17, 0, 2, 0, 73, 68, 0, 0, 
        4, 0, 0, 0, 18, 0, 2, 0, 
        78, 85, 77, 66, 69, 82, 0, 0, 
        6, 0, 0, 0, 50, 0, 2, 0, 
        -77, 6, 0, 0, -16, 4, 0, 0, 
        72, 0, 0, 0, 2, 0, 0, 0, 
        4, 0, 0, 0, 48, 0, 2, 0, 
        -94, -100, -104, 0, 0, 0, 0, 0, 
        3, 0, 0, 0, 16, 0, 2, 0, 
        2, 0, 0, 0, 4, 0, 0, 0, 
        17, 0, 2, 0, 68, 69, 84, 65, 
        73, 76, 83, 0, 4, 0, 0, 0, 
        18, 0, 2, 0, 67, 76, 79, 66, 
        0, 0, 0, 0, 6, 0, 0, 0, 
        50, 0, 2, 0, -77, 6, 0, 0, 
        -16, 4, 0, 0, 72, 0, 0, 0, 
        2, 0, 0, 0, 4, 0, 0, 0, 
        48, 0, 2, 0, -93, -100, -104, 0, 
        0, 0, 0, 0, 3, 0, 0, 0, 
        16, 0, 2, 0, 3, 0, 0, 0, 
        5, 0, 0, 0, 17, 0, 2, 0, 
        73, 77, 71, 95, 68, 69, 84, 65, 
        73, 76, 83, 0, 4, 0, 0, 0, 
        18, 0, 2, 0, 66, 76, 79, 66, 
        0, 0, 0, 0, 6, 0, 0, 0, 
        50, 0, 2, 0, -77, 6, 0, 0, 
        -16, 4, 0, 0, 72, 0, 0, 0, 
        2, 0, 0, 0, 4, 0, 0, 0, 
        48, 0, 2, 0, -87, -100, -104, 0, 
        0, 0, 0, 0, 88, 0, 0, 0, 
        11, 0, 0, 0, 0, 2, 0, 0, 
        7, 0, 0, 0, 1, 0, 3, 0, 
        48, 48, 48, 53, 46, 48, 49, 102, 
        46, 48, 48, 48, 48, 57, 98, 51, 
        50, 0, 0, 0, 3, 0, 0, 0, 
        32, 0, 2, 0, 19, -83, 0, 0, 
        4, 0, 0, 0, 34, 0, 2, 0, 
        83, 67, 79, 84, 84, 0, 0, 0, 
        4, 0, 0, 0, 33, 0, 2, 0, 
        84, 69, 83, 84, 50, 0, 0, 0, 
        4, 0, 0, 0, 2, 0, 3, 0, 
        45, -114, -61, 1, 0, 0, 0, 0, 
        3, 0, 0, 0, 16, 0, 2, 0, 
        1, 0, 0, 0, 3, 0, 0, 0, 
        17, 0, 2, 0, 73, 68, 0, 0, 
        4, 0, 0, 0, 18, 0, 2, 0, 
        78, 85, 77, 66, 69, 82, 0, 0, 
        6, 0, 0, 0, 50, 0, 2, 0, 
        -77, 6, 0, 0, -127, 13, 0, 0, 
        0, 0, 0, 0, 1, 0, 0, 0, 
        4, 0, 0, 0, 48, 0, 2, 0, 
        -17, -93, -104, 0, 0, 0, 0, 0, 
        3, 0, 0, 0, 16, 0, 2, 0, 
        2, 0, 0, 0, 4, 0, 0, 0, 
        17, 0, 2, 0, 68, 69, 84, 65, 
        73, 76, 83, 0, 4, 0, 0, 0, 
        18, 0, 2, 0, 67, 76, 79, 66, 
        0, 0, 0, 0, 6, 0, 0, 0, 
        50, 0, 2, 0, -77, 6, 0, 0, 
        -127, 13, 0, 0, 0, 0, 0, 0, 
        1, 0, 0, 0, 4, 0, 0, 0, 
        48, 0, 2, 0, -16, -93, -104, 0, 
        0, 0, 0, 0, 3, 0, 0, 0, 
        16, 0, 2, 0, 3, 0, 0, 0, 
        5, 0, 0, 0, 17, 0, 2, 0, 
        73, 77, 71, 95, 68, 69, 84, 65, 
        73, 76, 83, 0, 4, 0, 0, 0, 
        18, 0, 2, 0, 66, 76, 79, 66, 
        0, 0, 0, 0, 6, 0, 0, 0, 
        50, 0, 2, 0, -77, 6, 0, 0, 
        -127, 13, 0, 0, 0, 0, 0, 0, 
        1, 0, 0, 0, 4, 0, 0, 0, 
        48, 0, 2, 0, -10, -93, -104, 0, 
        0, 0, 0, 0, 47, 0, 0, 0, 
        11, 0, 0, 0, 2, 0, 0, 0, 
        7, 0, 0, 0, 1, 0, 3, 0, 
        48, 48, 48, 53, 46, 48, 49, 102, 
        46, 48, 48, 48, 48, 57, 98, 51, 
        50, 0, 0, 0, 3, 0, 0, 0, 
        32, 0, 2, 0, 19, -83, 0, 0, 
        4, 0, 0, 0, 48, 0, 2, 0, 
        -7, -93, -104, 0, 0, 0, 0, 0, 
        6, 0, 0, 0, 50, 0, 2, 0, 
        -77, 6, 0, 0, 114, 13, 0, 0, 
        16, 1, 0, 0, 2, 0, 0, 0, 
        4, 0, 0, 0, 2, 0, 3, 0, 
        36, -114, -61, 1, 0, 0, 0, 0, 
        3, 0, 0, 0, 3, 0, 3, 0, 
        59, -12, 118, 55, 3, 0, 0, 0, 
        16, 0, 2, 0, 1, 0, 0, 0, 
        4, 0, 0, 0, 2, 0, 2, 0, 
        2, 0, 0, 0, -63, 2, 0, 0, 
        3, 0, 0, 0, 16, 0, 2, 0, 
        2, 0, 0, 0, 3, 0, 0, 0, 
        16, 0, 2, 0, 3, 0, 0, 0, 
        4, 0, 0, 0, 2, 1, 32, 0, 
        2, 0, 0, 0, 1, 16, 0, 0, 
        -52, 0, 0, 0, 11, 0, 0, 0, 
        16, 0, 0, 0, 7, 0, 0, 0, 
        1, 0, 3, 0, 48, 48, 48, 53, 
        46, 48, 49, 102, 46, 48, 48, 48, 
        48, 57, 98, 51, 50, 0, 0, 0, 
        4, 0, 0, 0, 2, 0, 3, 0, 
        36, -114, -61, 1, 0, 0, 0, 0, 
        3, 0, 0, 0, 3, 0, 3, 0, 
        59, -12, 118, 55, 3, 0, 0, 0, 
        32, 0, 2, 0, 19, -83, 0, 0, 
        4, 0, 0, 0, 48, 0, 2, 0, 
        -6, -93, -104, 0, 0, 0, 0, 0, 
        6, 0, 0, 0, 50, 0, 2, 0, 
        -77, 6, 0, 0, 114, 13, 0, 0, 
        16, 1, 0, 0, 3, 0, 1, 0, 
        3, 0, 0, 0, 16, 0, 2, 0, 
        2, 0, 0, 0, 4, 0, 0, 0, 
        17, 0, 2, 0, 68, 69, 84, 65, 
        73, 76, 83, 0, 5, 0, 0, 0, 
        18, 0, 2, 0, 67, 76, 79, 66, 
        95, 85, 84, 70, 49, 54, 0, 0, 
        3, 0, 0, 0, 6, 0, 2, 0, 
        1, 0, 0, 0, 4, 0, 0, 0, 
        5, 0, 2, 0, 1, 0, 0, 0, 
        0, 0, 0, 0, 6, 0, 0, 0, 
        8, 0, 2, 0, 10, 0, 0, 0, 
        0, 0, 0, 1, 0, 0, 0, 82, 
        98, -94, 0, 0, 4, 0, 0, 0, 
        7, 0, 2, 0, -20, 3, 0, 0, 
        0, 0, 0, 0, -126, 0, 0, 0, 
        4, 0, 2, 0, -10, 1, 0, 0, 
        0, 0, 0, 0, 116, 104, 105, 115, 
        32, 105, 115, 32, 115, 111, 109, 101, 
        32, 100, 101, 116, 97, 105, 108, 32, 
        116, 104, 97, 116, 32, 73, 32, 97, 
        109, 32, 97, 100, 100, 105, 110, 103, 
        32, 105, 110, 32, 97, 110, 100, 32, 
        73, 32, 97, 109, 32, 110, 111, 119, 
        32, 103, 111, 105, 110, 103, 32, 116, 
        111, 32, 103, 111, 32, 97, 104, 101, 
        97, 100, 32, 97, 110, 100, 32, 97, 
        100, 100, 32, 97, 32, 108, 111, 116, 
        32, 111, 102, 32, 106, 117, 110, 107, 
        32, 105, 110, 32, 104, 101, 114, 101, 
        46, 46, 46, 66, 117, 116, 32, 97, 
        115, 32, 119, 101, 32, 114, 101, 102, 
        108, 101, 99, 116, 101, 100, 32, 109, 
        111, 114, 101, 32, 111, 110, 32, 119, 
        104, 111, 32, 119, 101, 32, 97, 114, 
        101, 32, 97, 115, 32, 97, 32, 99, 
        111, 109, 112, 97, 110, 121, 32, 45, 
        32, 97, 32, 98, 117, 110, 99, 104, 
        32, 111, 102, 32, 114, 101, 97, 108, 
        108, 121, 32, 105, 110, 116, 101, 114, 
        101, 115, 116, 105, 110, 103, 44, 32, 
        104, 97, 114, 100, 32, 119, 111, 114, 
        107, 105, 110, 103, 44, 32, 105, 110, 
        110, 111, 118, 97, 116, 105, 118, 101, 
        32, 97, 110, 100, 32, 99, 97, 114, 
        105, 110, 103, 32, 112, 101, 111, 112, 
        108, 101, 32, 45, 32, 119, 101, 32, 
        114, 101, 97, 108, 105, 115, 101, 100, 
        32, 116, 104, 97, 116, 32, 119, 104, 
        97, 116, 32, 119, 97, 115, 32, 109, 
        105, 115, 115, 105, 110, 103, 32, 119, 
        97, 115, 32, 117, 115, 46, 32, 87, 
        101, 32, 104, 97, 118, 101, 32, 97, 
        110, 32, 105, 110, 116, 101, 114, 101, 
        115, 116, 105, 110, 103, 32, 115, 116, 
        111, 114, 121, 32, 97, 115, 32, 97, 
        32, 100, 105, 115, 116, 114, 105, 98, 
        117, 116, 101, 100, 32, 99, 111, 109, 
        112, 97, 110, 121, 32, 111, 102, 32, 
        105, 110, 116, 101, 114, 110, 97, 116, 
        105, 111, 110, 97, 108, 115, 32, 102, 
        111, 117, 110, 100, 101, 100, 32, 105, 
        110, 32, 65, 117, 99, 107, 108, 97, 
        110, 100, 44, 32, 78, 90, 46, 32, 
        87, 101, 32, 115, 101, 101, 32, 116, 
        104, 105, 110, 103, 115, 32, 97, 32, 
        108, 105, 116, 116, 108, 101, 32, 100, 
        105, 102, 102, 101, 114, 101, 110, 116, 
        32, 45, 32, 97, 110, 100, 32, 111, 
        117, 114, 32, 108, 111, 99, 97, 116, 
        105, 111, 110, 32, 97, 110, 100, 32, 
        100, 105, 115, 116, 97, 110, 99, 101, 
        32, 105, 115, 32, 97, 32, 115, 116, 
        114, 101, 110, 103, 116, 104, 32, 116, 
        104, 97, 116, 32, 119, 101, 32, 108, 
        101, 118, 101, 114, 97, 103, 101, 32, 
        45, 32, 119, 101, 32, 97, 114, 101, 
        32, 116, 104, 111, 115, 101, 32, 110, 
        105, 109, 98, 108, 101, 32, 111, 117, 
        116, 115, 105, 100, 101, 114, 115, 46, 
        46, 32, 0, 0, 3, 0, 0, 0, 
        16, 0, 2, 0, 1, 0, 0, 0, 
        4, 0, 0, 0, 3, 0, 2, 0, 
        2, 0, 0, 0, -63, 2, 0, 0, 
        4, 0, 0, 0, 3, 1, 32, 0, 
        2, 0, 0, 0, 1, 16, 0, 0, 
        4, 0, 0, 0, 2, 1, 32, 0, 
        2, 0, 0, 0, 2, 16, 0, 0, 
        72, 1, 0, 0, 11, 0, 0, 0, 
        16, 0, 0, 0, 7, 0, 0, 0, 
        1, 0, 3, 0, 48, 48, 48, 53, 
        46, 48, 49, 102, 46, 48, 48, 48, 
        48, 57, 98, 51, 50, 0, 0, 0, 
        4, 0, 0, 0, 2, 0, 3, 0, 
        36, -114, -61, 1, 0, 0, 0, 0, 
        3, 0, 0, 0, 3, 0, 3, 0, 
        59, -12, 118, 55, 3, 0, 0, 0, 
        32, 0, 2, 0, 19, -83, 0, 0, 
        4, 0, 0, 0, 48, 0, 2, 0, 
        -5, -93, -104, 0, 0, 0, 0, 0, 
        6, 0, 0, 0, 50, 0, 2, 0, 
        -77, 6, 0, 0, 114, 13, 0, 0, 
        16, 1, 0, 0, 3, 0, 2, 0, 
        3, 0, 0, 0, 16, 0, 2, 0, 
        3, 0, 0, 0, 5, 0, 0, 0, 
        17, 0, 2, 0, 73, 77, 71, 95, 
        68, 69, 84, 65, 73, 76, 83, 0, 
        4, 0, 0, 0, 18, 0, 2, 0, 
        66, 76, 79, 66, 0, 0, 0, 0, 
        3, 0, 0, 0, 6, 0, 2, 0, 
        1, 0, 0, 0, 4, 0, 0, 0, 
        5, 0, 2, 0, 1, 0, 0, 0, 
        0, 0, 0, 0, 6, 0, 0, 0, 
        8, 0, 2, 0, 10, 0, 0, 0, 
        0, 0, 0, 1, 0, 0, 0, 82, 
        98, -93, 0, 0, 4, 0, 0, 0, 
        7, 0, 2, 0, -24, 3, 0, 0, 
        0, 0, 0, 0, -2, 0, 0, 0, 
        4, 0, 2, 0, -24, 3, 0, 0, 
        0, 0, 0, 0, 86, 85, 77, 86, 
        72, 72, 80, 77, 67, 88, 88, 69, 
        89, 81, 67, 67, 90, 68, 76, 90, 
        85, 90, 80, 71, 70, 80, 76, 65, 
        79, 85, 78, 83, 75, 69, 88, 82, 
        87, 65, 73, 74, 80, 67, 85, 85, 
        66, 69, 84, 67, 67, 68, 74, 88, 
        89, 84, 81, 65, 74, 89, 87, 86, 
        66, 67, 69, 78, 66, 85, 87, 86, 
        82, 75, 77, 79, 70, 71, 84, 86, 
        68, 74, 73, 73, 83, 86, 89, 77, 
        78, 68, 84, 85, 67, 84, 78, 89, 
        68, 77, 88, 81, 88, 82, 80, 83, 
        79, 70, 81, 73, 75, 68, 84, 66, 
        68, 89, 74, 67, 85, 85, 72, 80, 
        71, 88, 73, 69, 78, 75, 84, 85, 
        88, 74, 70, 67, 89, 74, 79, 82, 
        80, 90, 81, 67, 90, 82, 79, 81, 
        67, 67, 87, 87, 78, 86, 78, 75, 
        75, 70, 86, 76, 66, 73, 78, 76, 
        80, 87, 76, 73, 78, 72, 84, 83, 
        88, 90, 69, 85, 77, 86, 71, 74, 
        65, 70, 66, 66, 70, 71, 69, 72, 
        79, 65, 78, 65, 83, 66, 68, 69, 
        86, 66, 76, 78, 90, 72, 74, 65, 
        67, 66, 82, 65, 81, 84, 84, 76, 
        75, 87, 77, 71, 80, 83, 84, 69, 
        87, 89, 73, 70, 74, 88, 88, 84, 
        66, 80, 72, 77, 68, 80, 75, 67, 
        71, 83, 66, 87, 65, 89, 83, 90, 
        87, 65, 72, 72, 84, 88, 73, 82, 
        65, 84, 83, 73, 69, 71, 84, 68, 
        82, 90, 77, 85, 74, 68, 78, 82, 
        79, 79, 67, 80, 81, 77, 68, 83, 
        90, 80, 78, 67, 85, 70, 75, 67, 
        74, 65, 85, 73, 86, 68, 70, 88, 
        82, 79, 81, 74, 67, 71, 82, 76, 
        79, 75, 88, 75, 74, 77, 75, 90, 
        74, 71, 76, 89, 77, 69, 70, 65, 
        90, 72, 68, 69, 72, 84, 80, 71, 
        67, 84, 90, 84, 83, 86, 68, 78, 
        90, 76, 78, 68, 83, 85, 66, 84, 
        72, 71, 72, 73, 73, 76, 89, 82, 
        86, 84, 79, 74, 90, 71, 82, 82, 
        68, 88, 68, 66, 72, 79, 78, 74, 
        82, 89, 67, 69, 90, 72, 85, 72, 
        78, 75, 77, 80, 69, 78, 88, 88, 
        78, 78, 67, 82, 66, 85, 69, 68, 
        73, 81, 70, 65, 74, 80, 68, 90, 
        69, 75, 77, 72, 84, 84, 89, 73, 
        69, 66, 90, 68, 85, 79, 79, 82, 
        76, 71, 84, 74, 73, 83, 78, 65, 
        80, 73, 69, 73, 87, 89, 71, 83, 
        85, 90, 87, 88, 72, 86, 71, 82, 
        80, 66, 86, 87, 74, 84, 85, 84, 
        88, 90, 74, 83, 72, 82, 90, 85, 
        83, 82, 80, 81, 82, 69, 65, 90, 
        66, 87, 66, 66, 75, 86, 74, 65, 
        73, 80, 70, 83, 77, 73, 85, 77, 
        72, 79, 65, 69, 80, 70, 77, 77, 
        82, 77, 78, 89, 65, 71, 82, 81, 
        88, 88, 88, 84, 79, 68, 84, 70, 
        79, 80, 75, 85, 90, 85, 72, 90, 
        71, 81, 85, 71, 75, 78, 76, 83, 
        73, 80, 90, 75, 66, 65, 81, 70, 
        78, 68, 77, 66, 77, 79, 82, 87, 
        68, 75, 89, 79, 65, 84, 77, 88, 
        67, 73, 69, 76, 84, 68, 73, 71, 
        78, 88, 69, 81, 68, 75, 76, 66, 
        83, 87, 86, 76, 73, 89, 86, 75, 
        65, 84, 86, 75, 71, 89, 80, 76, 
        89, 69, 86, 86, 68, 89, 77, 65, 
        65, 81, 82, 81, 89, 68, 89, 87, 
        72, 84, 65, 73, 83, 73, 72, 68, 
        66, 65, 86, 65, 66, 88, 83, 76, 
        66, 65, 77, 72, 74, 88, 67, 83, 
        78, 78, 67, 71, 66, 84, 71, 73, 
        77, 86, 84, 90, 71, 87, 80, 90, 
        69, 82, 86, 69, 87, 70, 76, 67, 
        82, 69, 88, 72, 65, 65, 79, 85, 
        72, 67, 80, 85, 66, 78, 76, 79, 
        86, 79, 90, 72, 85, 72, 76, 71, 
        83, 73, 76, 70, 67, 78, 85, 69, 
        82, 90, 79, 67, 85, 86, 67, 85, 
        89, 73, 84, 73, 75, 66, 79, 65, 
        71, 86, 77, 82, 77, 88, 73, 74, 
        77, 74, 78, 68, 79, 74, 89, 89, 
        66, 68, 88, 87, 73, 79, 73, 85, 
        88, 83, 80, 69, 74, 90, 72, 90, 
        68, 88, 87, 80, 76, 68, 78, 68, 
        73, 66, 71, 73, 69, 65, 83, 65, 
        77, 81, 69, 83, 80, 88, 85, 69, 
        79, 67, 86, 87, 87, 80, 73, 80, 
        72, 74, 84, 90, 78, 77, 66, 75, 
        70, 68, 70, 78, 79, 66, 85, 74, 
        74, 85, 88, 89, 87, 66, 68, 82, 
        65, 83, 77, 73, 84, 87, 84, 80, 
        76, 65, 73, 83, 77, 85, 75, 83, 
        85, 74, 71, 69, 89, 79, 79, 88, 
        87, 84, 86, 84, 82, 76, 71, 72, 
        67, 71, 72, 72, 73, 85, 65, 82, 
        69, 78, 71, 66, 86, 70, 67, 68, 
        69, 69, 68, 86, 81, 82, 79, 87, 
        77, 72, 66, 76, 73, 90, 87, 78, 
        71, 80, 65, 86, 80, 75, 74, 89, 
        88, 77, 70, 84, 85, 81, 65, 66, 
        88, 89, 79, 73, 68, 86, 69, 79, 
        78, 74, 83, 81, 85, 87, 70, 75, 
        68, 71, 87, 75, 81, 76, 66, 66, 
        81, 73, 79, 75, 72, 79, 89, 75, 
        70, 81, 84, 76, 86, 65, 67, 85, 
        90, 83, 76, 75, 72, 79, 74, 65, 
        83, 67, 69, 75, 66, 67, 67, 79, 
        72, 68, 83, 76, 75, 67, 89, 84, 
        65, 77, 67, 81, 88, 72, 69, 67, 
        89, 73, 85, 88, 65, 75, 66, 74, 
        76, 83, 85, 74, 81, 65, 89, 78, 
        85, 76, 87, 70, 67, 66, 78, 90, 
        69, 79, 65, 70, 87, 79, 67, 82, 
        75, 90, 72, 67, 78, 69, 89, 83, 
        87, 78, 85, 66, 67, 88, 71, 86, 
        88, 90, 86, 65, 74, 83, 67, 68, 
        87, 86, 67, 88, 90, 69, 66, 87, 
        82, 67, 76, 68, 69, 88, 70, 81, 
        90, 82, 71, 70, 90, 85, 86, 67, 
        78, 87, 70, 71, 3, 0, 0, 0, 
        16, 0, 2, 0, 1, 0, 0, 0, 
        4, 0, 0, 0, 3, 0, 2, 0, 
        2, 0, 0, 0, -63, 2, 0, 0, 
        4, 0, 0, 0, 3, 1, 32, 0, 
        2, 0, 0, 0, 1, 16, 0, 0, 
        4, 0, 0, 0, 2, 1, 32, 0, 
        2, 0, 0, 0, 3, 16, 0, 0, 
        14, 0, 0, 0, 0, 0, 0, 0, 
        1, 0, 0, 0, 3, 0, 0, 0, 
        4, 0, 0, 0, 1, 0, 0, 0, 
        3, 0, 0, 0, 2, 0, 0, 0, 
        -57, 86, -53, 64, 5, 0, 0, 0, 
        5, 0, 0, 0, 80, 76, 79, 71, 
        32, 69, 78, 68, 0, 0, 0, 0
    };

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
}
