package com.dbvisit.replicate.plog.domain.parser;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;
import java.sql.Timestamp;

import javax.sql.rowset.serial.SerialBlob;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.plog.domain.ChangeAction;
import com.dbvisit.replicate.plog.domain.ColumnValue;
import com.dbvisit.replicate.plog.domain.LogicalChangeRecord;
import com.dbvisit.replicate.plog.format.EntryRecord;
import com.dbvisit.replicate.plog.metadata.Column;
import com.dbvisit.replicate.plog.metadata.DDLMetaData;

/** Test parsing single entry record to LCR, verifies PLOG parsing of 
 *  Entry Records as well.
 */
public class LogicalChangeParserTest extends ChangeParserTestConfig {
    private static final Logger logger = LoggerFactory.getLogger(
        LogicalChangeParserTest.class
    );
    
    private final int LCR_INSERT_NUM_ENTRY_RECORDS    = 6;
    private final int LCR_LOB_WRITE_NUM_ENTRY_RECORDS = 7;
    
    private final ChangeAction[] LCR_INSERT_CHANGE_ACTIONS = {
        ChangeAction.NONE,         /* DDL METADATA_RECORD as LCR for CREATE */
        ChangeAction.NO_OPERATION, /* CREATE DDL NOOP */
        ChangeAction.NO_OPERATION, /* first INSERT's NOOP */
        ChangeAction.INSERT,       /* INSERT */
        ChangeAction.NONE,         /* DDL METADATA_RECORD as LCR for DROP */
        ChangeAction.NO_OPERATION  /* DROP DDL NOOP */
    };
    
    private final ChangeAction[] LCR_LOB_WRITE_CHANGE_ACTIONS = {
        ChangeAction.NONE,         /* DDL METADATA_RECORD as LCR for CREATE */
        ChangeAction.NO_OPERATION, /* CREATE DDL NOOP */
        ChangeAction.NO_OPERATION, /* first INSERT's NOOP */
        ChangeAction.INSERT,       /* INSERT */
        ChangeAction.LOB_WRITE,    /* LOB_WRITE */
        ChangeAction.NONE,         /* DDL METADATA_RECORD as LCR for DROP */
        ChangeAction.NO_OPERATION  /* DROP DDL NOOP */
    };
    
    @Test
    public void testParseIntegerLCR() {
        /* UNITTEST SQL:
          
           CREATE TABLE SOE.UNITTEST (TEST NUMBER(4));
           INSERT INTO SOE.UNITTEST VALUES (9999);
           COMMIT;
           DROP TABLE SOE.UNITTEST;
           COMMIT;
        */
        final String COLUMN_TYPE = "NUMBER";
        final int COLUMN_PRECISION = 4;
        final int COLUMN_SCALE = 0;
        final Object COLUMN_VALUE = 9999;

        testParseInsertLCR (
            integerValueLCR(),
            COLUMN_TYPE,
            COLUMN_PRECISION,
            COLUMN_SCALE,
            COLUMN_VALUE
        );
    }
    
    @Test
    public void testParseLongLCR() {
        /* UNITTEST SQL:
          
          CREATE TABLE SOE.UNITTEST (TEST NUMBER(18));
          INSERT INTO SOE.UNITTEST VALUES (99999999999999999);
          COMMIT;
          DROP TABLE SOE.UNITTEST;
          COMMIT;
        */
        final String COLUMN_TYPE = "NUMBER";
        final int COLUMN_PRECISION = 18;
        final int COLUMN_SCALE = 0;
        final Object COLUMN_VALUE = 99999999999999999L;
        
        testParseInsertLCR (
            longValueLCR(),
            COLUMN_TYPE,
            COLUMN_PRECISION,
            COLUMN_SCALE,
            COLUMN_VALUE
        );
    }
    
    @Test
    public void testParseNumberLCR() {
        /* UNITTEST SQL:
          
           CREATE TABLE SOE.UNITTEST (TEST NUMBER(12, 5));
           INSERT INTO SOE.UNITTEST VALUES ('9999999.99999');
           COMMIT;
           DROP TABLE SOE.UNITTEST;
           COMMIT;
        */
        final String COLUMN_TYPE = "NUMBER";
        final int COLUMN_PRECISION = 12;
        final int COLUMN_SCALE = 5;
        final Object COLUMN_VALUE = new BigDecimal("9999999.99999");

        testParseInsertLCR (
            numberValueLCR(),
            COLUMN_TYPE,
            COLUMN_PRECISION,
            COLUMN_SCALE,
            COLUMN_VALUE
        );
    }
    
    @Test
    public void testParseVarcharLCR() {
        /* UNITTEST SQL:

           CREATE TABLE SOE.UNITTEST (TEST VARCHAR2(255));
           INSERT INTO SOE.UNITTEST VALUES ('Test character string to decode - 0123456789 - ~!@#$%^&*()_+');
           COMMIT;
           DROP TABLE SOE.UNITTEST;
           COMMIT;
        */
        final String COLUMN_TYPE = "VARCHAR2";
        final int COLUMN_PRECISION = -1;
        final int COLUMN_SCALE = -1;
        final Object COLUMN_VALUE = "Test character string to decode - 0123456789 - ~!@#$%^&*()_+";

        testParseInsertLCR (
            varcharValueLCR(),
            COLUMN_TYPE,
            COLUMN_PRECISION,
            COLUMN_SCALE,
            COLUMN_VALUE
        );
    }
    
    @Test
    public void testParseNVarcharLCR() {
        /* UNITTEST SQL:

           CREATE TABLE SOE.UNITTEST (TEST NVARCHAR2(255));
           INSERT INTO SOE.UNITTEST VALUES ('Test character string to decode - 0123456789 - ~!@#$%^&*()_+');
           COMMIT;
           DROP TABLE SOE.UNITTEST;
           COMMIT;
        */
        final String COLUMN_TYPE = "NVARCHAR2";
        final int COLUMN_PRECISION = -1;
        final int COLUMN_SCALE = -1;
        final Object COLUMN_VALUE = "Test character string to decode - 0123456789 - ~!@#$%^&*()_+";

        testParseInsertLCR (
            nvarcharValueLCR(),
            COLUMN_TYPE,
            COLUMN_PRECISION,
            COLUMN_SCALE,
            COLUMN_VALUE
        );
    }
    
    @Test
    public void testParseLongCharLCR() {
        /* UNITTEST SQL:

           CREATE TABLE SOE.UNITTEST (TEST LONG);
           INSERT INTO SOE.UNITTEST VALUES ('LONG test character string to decode - 0123456789 - ~!@#$%^&*()_+');
           COMMIT;
           DROP TABLE SOE.UNITTEST;
           COMMIT;
        */
        final String COLUMN_TYPE = "LONG";
        final int COLUMN_PRECISION = -1;
        final int COLUMN_SCALE = -1;
        final Object COLUMN_VALUE = "LONG test character string to decode - 0123456789 - ~!@#$%^&*()_+";

        testParseInsertLCR (
            longCharValueLCR(),
            COLUMN_TYPE,
            COLUMN_PRECISION,
            COLUMN_SCALE,
            COLUMN_VALUE
        );
    }
    
    @Test
    public void testParseRawBinaryLCR() {
        /* UNITTEST SQL:

           CREATE TABLE SOE.UNITTEST (TEST RAW(200));
           INSERT INTO SOE.UNITTEST VALUES ('0102030405060708090A0B0C0D0E0F');
           COMMIT;
           SELECT * FROM SOE.UNITTEST;
           DROP TABLE SOE.UNITTEST;
           COMMIT;
        */
        final String COLUMN_TYPE = "RAW";
        final int COLUMN_PRECISION = -1;
        final int COLUMN_SCALE = -1;
        Object COLUMN_VALUE = "0102030405060708090A0B0C0D0E0F";
        
        testParseInsertLCR (
            rawBinaryValueLCR(),
            COLUMN_TYPE,
            COLUMN_PRECISION,
            COLUMN_SCALE,
            COLUMN_VALUE
        );
    }
    
    @Test
    public void testParseDateLCR() {
        /* UNITTEST SQL:

           CREATE TABLE SOE.UNITTEST (TEST DATE);
           INSERT INTO SOE.UNITTEST VALUES (TO_DATE('2016/09/01', 'YYYY/MM/DD'));
           COMMIT;
           SELECT * FROM SOE.UNITTEST;
           DROP TABLE SOE.UNITTEST;
           COMMIT;
        */
        final String COLUMN_TYPE = "DATE";
        final int COLUMN_PRECISION = -1;
        final int COLUMN_SCALE = -1;
        
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
        Object COLUMN_VALUE = null;
        try {
            Date d = dateFormat.parse("2016/09/01");
            COLUMN_VALUE = new Timestamp (d.getTime());
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
        
        testParseInsertLCR (
            dateValueLCR(),
            COLUMN_TYPE,
            COLUMN_PRECISION,
            COLUMN_SCALE,
            COLUMN_VALUE
        );
    }
    
    @Test
    public void testParseTimestampLCR() {
        /* UNITTEST SQL:

           CREATE TABLE SOE.UNITTEST (TEST TIMESTAMP);
           INSERT INTO SOE.UNITTEST VALUES (TO_TIMESTAMP('2016/09/01 00:00:01', 'YYYY/MM/DD HH24:MI:SS'));
           COMMIT;
           DROP TABLE SOE.UNITTEST;
           COMMIT;
        */
        final String COLUMN_TYPE = "TIMESTAMP";
        final int COLUMN_PRECISION = -1;
        final int COLUMN_SCALE = 6;
        
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss");
        Object COLUMN_VALUE = null;
        try {
            Date d = dateFormat.parse("2016/09/01 00:00:01");
            COLUMN_VALUE = new Timestamp (d.getTime());
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
        
        testParseInsertLCR (
            timestampValueLCR(),
            COLUMN_TYPE,
            COLUMN_PRECISION,
            COLUMN_SCALE,
            COLUMN_VALUE
        );
    }
    
    @Test
    public void testParseTimestampTzLCR() {
        /* UNITTEST SQL:

           CREATE TABLE SOE.UNITTEST (TEST TIMESTAMP WITH TIME ZONE);
           INSERT INTO SOE.UNITTEST VALUES (TO_TIMESTAMP_TZ('2016/09/01 00:00:01 +12:00', 'YYYY/MM/DD HH24:MI:SS TZH:TZM'));
           COMMIT;
           DROP TABLE SOE.UNITTEST;
        */
        final String COLUMN_TYPE = "TIMESTAMP WITH TIME ZONE";
        final int COLUMN_PRECISION = -1;
        final int COLUMN_SCALE = 6;
        
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("Pacific/Auckland"));
        
        DateFormat dateFormatUTC = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss");
        dateFormatUTC.setTimeZone(TimeZone.getTimeZone("UTC"));
        
        Object COLUMN_VALUE = null;
        try {
            Date dtz = dateFormat.parse("2016/09/01 00:00:01");
            Date d = dateFormatUTC.parse(dateFormatUTC.format(dtz));
            
            COLUMN_VALUE = new Timestamp (d.getTime());
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
        
        testParseInsertLCR (
            timestampTzValueLCR(),
            COLUMN_TYPE,
            COLUMN_PRECISION,
            COLUMN_SCALE,
            COLUMN_VALUE
        );
    }
    
    @Test
    public void testParseIntervalDayToSecLCR() {
        /* UNITTEST SQL:

           CREATE TABLE SOE.UNITTEST (TEST INTERVAL DAY(3) TO SECOND (2));
           INSERT INTO SOE.UNITTEST VALUES (INTERVAL '123 2:25:45.12' DAY(3) TO SECOND(2));
           COMMIT;
           DROP TABLE SOE.UNITTEST;
           COMMIT;
        */
        final String COLUMN_TYPE = "INTERVAL DAY TO SECOND";
        final int COLUMN_PRECISION = 3;
        final int COLUMN_SCALE = 2;
        
        /*
         SELECT INTERVAL '123 2:25:45.12' DAY(3) TO SECOND(2) FROM DUAL;
         ----------------------------------------------------------------------
         +123 02:25:45.12
         
         Add formatting
         */
        Object COLUMN_VALUE = "+123 2:25:45.120000000";
        
        testParseInsertLCR (
            intervalDayToSecValueLCR(),
            COLUMN_TYPE,
            COLUMN_PRECISION,
            COLUMN_SCALE,
            COLUMN_VALUE
        );
    }
    
    @Test
    public void testParseIntervalYearToMonthLCR() {
        /* UNITTEST SQL:
           
           CREATE TABLE SOE.UNITTEST (TEST INTERVAL YEAR(3) TO MONTH);
           INSERT INTO SOE.UNITTEST VALUES (INTERVAL '1-3' YEAR TO MONTH);
           COMMIT;
           DROP TABLE SOE.UNITTEST;
           COMMIT;
        */
        final String COLUMN_TYPE = "INTERVAL YEAR TO MONTH";
        final int COLUMN_PRECISION = 3;
        final int COLUMN_SCALE = 0;
        
        /*
         SELECT INTERVAL '1-3' YEAR TO MONTH FROM DUAL;
         ----------------------------------------------------------------------
         +01-03
         
         Add formatting
         */
        Object COLUMN_VALUE = "+1-3";
        
        testParseInsertLCR (
            intervalYearToMonthValueLCR(),
            COLUMN_TYPE,
            COLUMN_PRECISION,
            COLUMN_SCALE,
            COLUMN_VALUE
        );
    }
    
    @Test
    public void testParseClobLCR() {
        /* UNITTEST SQL:
         
           CREATE TABLE SOE.UNITTEST (TEST CLOB);
           INSERT INTO SOE.UNITTEST VALUES (TO_CLOB ('<html><body><p>This is a HTML paragraph</p></body></html>'));
           COMMIT;
           DROP TABLE SOE.UNITTEST;
           COMMIT;
         */
        final String COLUMN_TYPE = "CLOB";
        
        Object COLUMN_VALUE = "<html><body><p>This is a HTML paragraph</p></body></html>";
        
        testParseLobWriteLCR (
            clobValueLCR(),
            COLUMN_TYPE,
            COLUMN_VALUE
        );
    }
    
    @Test
    public void testParseNClobLCR() {
        /* UNITTEST SQL:
         
           CREATE TABLE SOE.UNITTEST (TEST NCLOB);
           INSERT INTO SOE.UNITTEST VALUES (TO_NCLOB ('<html><body><p>This is a HTML paragraph</p></body></html>'));
           COMMIT;
           DROP TABLE SOE.UNITTEST;
           COMMIT;
         */
        final String COLUMN_TYPE = "NCLOB";
        
        Object COLUMN_VALUE = "<html><body><p>This is a HTML paragraph</p></body></html>";
        
        testParseLobWriteLCR (
            nclobValueLCR(),
            COLUMN_TYPE,
            COLUMN_VALUE
        );
    }
    
    @Test
    public void testParseBlobLCR() {
        /* UNITTEST SQL:
         
           CREATE TABLE SOE.UNITTEST (TEST BLOB);
           INSERT INTO SOE.UNITTEST VALUES (TO_BLOB('FF3311121212EE3A'));
           COMMIT;
           DROP TABLE SOE.UNITTEST;
           COMMIT;
         */
        final String COLUMN_TYPE = "BLOB";
        
        Object COLUMN_VALUE = "FF3311121212EE3A";
        
        testParseLobWriteLCR (
            blobValueLCR(),
            COLUMN_TYPE,
            COLUMN_VALUE
        );
    }
    
    public void testParseInsertLCR(
        byte[] lcrData,
        String columnType,
        int columnPrecision,
        int columnScale,
        Object columnValue
    ) {
        boolean createDDL = true;
        
        try {
            List<EntryRecord> records = parseEntryRecord (lcrData);
            
            assertTrue (
                "Expecting " + LCR_INSERT_NUM_ENTRY_RECORDS + " entry records, " +
                "got: " + records.size(),
                records.size() == LCR_INSERT_NUM_ENTRY_RECORDS
            );
            
            LogicalChangeParser lp = new LogicalChangeParser();
            
            for (int i = 0; i < LCR_INSERT_NUM_ENTRY_RECORDS; i++) {
                lp.parse(plog, records.get(i));
            
                LogicalChangeRecord lcr = (LogicalChangeRecord)lp.emit();

                logger.info ("[" + i + "]: " + lcr.toJSONString());
                
                assertTrue (
                    "Expecting LCR action: " + LCR_INSERT_CHANGE_ACTIONS[i] +
                    " , got: " + lcr.getAction(),
                    lcr.getAction().equals (LCR_INSERT_CHANGE_ACTIONS[i])
                );
                
                if (lcr.getAction().equals (ChangeAction.NONE)) {
                    if (createDDL) {
                        /* LCR NONE is META DATA */
                        assertTrue (
                            "Expecting one cached PLOG schema: " + LCR_SCHEMA + 
                            " ,got: " + plog.getSchemas().size() + " schemas, " +
                            plog.getSchemas().keySet().toString(),
                            plog.getSchemas().size() == 1 &&
                            plog.getSchemas().containsKey(LCR_SCHEMA)
                        );
                        createDDL = false;
                        
                        DDLMetaData ddl = plog.getSchemas().get (LCR_SCHEMA);
                        
                        assertTrue (
                            "Expecting DDL MetaData for: " + LCR_SCHEMA + ", " +
                            "got: " + ddl.getSchemataName(),
                            ddl.getSchemataName().equals (LCR_SCHEMA)
                        );
                        
                        assertTrue (
                            "Expecting " + NUM_COLUMNS + "columns, got: " +
                            ddl.getTableColumns().size(),
                            ddl.getTableColumns().size() == NUM_COLUMNS
                        );
                        
                        Column c = ddl.getTableColumns().get(0);
                        
                        assertTrue (
                            "Expecting column name: " + COLUMN_NAME     +
                            " type: " + columnType + " precision: "     + 
                            columnPrecision + " scale: " + columnScale  +
                            " nullable: " + COLUMN_NULLABLE + ", got: " + 
                            c.toString() + " " + c.getPrecision()       +
                            " " + c.getScale() + " " + c.getNullable(),
                            c.getName().equals (COLUMN_NAME) &&
                            c.getType().equals (columnType) &&
                            c.getPrecision() == columnPrecision &&
                            c.getScale() == columnScale &&
                            c.getNullable() == COLUMN_NULLABLE
                        );
                    }
                    else {
                        /* drop DDL */
                        assertTrue (
                            "Expecting two cached PLOG schemas when " + 
                            "dropping: " + LCR_SCHEMA + " ,got: " + 
                            plog.getSchemas().size() + " schemas, " +
                            plog.getSchemas().keySet().toString(),
                            plog.getSchemas().size() == 2 &&
                            plog.getSchemas().containsKey(LCR_SCHEMA)
                        );
                        
                        plog.getSchemas().remove (LCR_SCHEMA);
                        
                        Object []keys = plog.getSchemas().keySet().toArray();
                        String drop = (String)keys[0];
                        
                        assertTrue (
                            "Expecting second DDL to be for DROP table: " +
                            LCR_SCHEMA,
                            drop.startsWith("SOE.BIN")
                        );
                    }
                }
                
                if (lcr.getAction().equals (ChangeAction.INSERT)) {
                    /* for dumping out entry record raw data
                    System.out.print (
                        "int []" +
                        columnType + " (" + columnPrecision + ", " + 
                        columnScale + ") = "
                    );
                    int []data = 
                        records.get(i)
                               .getEntryTags()
                               .get(EntryTagType.TAG_POSTIMAGE)
                               .get(0)
                               .getRawData();
                    
                    System.out.print ("{ ");
                    for (int d = 0; d < data.length; d++) {
                        if (d > 0) {
                            System.out.print (", ");
                        }
                        System.out.print (data[d]);
                    }
                    System.out.println (" };");
                    */
                    
                    assertTrue (
                        "Expecting " + NUM_COLUMNS + " column values, got: " +
                        lcr.getColumnValues().size(),
                        lcr.getColumnValues().size() == NUM_COLUMNS
                    );
                    
                    ColumnValue cr = lcr.getColumnValues().get(0);
                    
                    assertTrue (
                        "Expecting column id: " + COLUMN_ID + " name: " + 
                        COLUMN_NAME + " value: " + columnValue + ", got: "+
                        cr.toString(),
                        cr.getId() == COLUMN_ID &&
                        cr.getName().equals(COLUMN_NAME) &&
                        (cr.getValue() instanceof SerialBlob 
                         ? cr.getValueAsString().equals(columnValue)
                         : cr.getValue().equals(columnValue))
                    );
                }
            }
            
            plog.getSchemas().clear();
            plog.getDictionary().clear();
            
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    public void testParseLobWriteLCR(
            byte[] lcrData,
            String columnType,
            Object columnValue
        ) {
            boolean createDDL = true;
            
            try {
                List<EntryRecord> records = parseEntryRecord (lcrData);
                
                assertTrue (
                    "Expecting " + LCR_LOB_WRITE_NUM_ENTRY_RECORDS + 
                    " entry records, " + "got: " + records.size(),
                    records.size() == LCR_LOB_WRITE_NUM_ENTRY_RECORDS
                );
                
                LogicalChangeParser lp = new LogicalChangeParser();
                
                for (int i = 0; i < LCR_LOB_WRITE_NUM_ENTRY_RECORDS; i++) {
                    lp.parse(plog, records.get(i));
                
                    LogicalChangeRecord lcr = (LogicalChangeRecord)lp.emit();

                    logger.info ("[" + i + "]: " + lcr.toJSONString());
                    
                    assertTrue (
                        "Expecting LCR action: " + LCR_LOB_WRITE_CHANGE_ACTIONS[i] +
                        " , got: " + lcr.getAction(),
                        lcr.getAction().equals (LCR_LOB_WRITE_CHANGE_ACTIONS[i])
                    );
                    
                    if (lcr.getAction().equals (ChangeAction.NONE)) {
                        if (createDDL) {
                            /* LCR NONE is META DATA */
                            assertTrue (
                                "Expecting one cached PLOG schema: " + LCR_SCHEMA + 
                                " ,got: " + plog.getSchemas().size() + " schemas, " +
                                plog.getSchemas().keySet().toString(),
                                plog.getSchemas().size() == 1 &&
                                plog.getSchemas().containsKey(LCR_SCHEMA)
                            );
                            createDDL = false;
                            
                            DDLMetaData ddl = plog.getSchemas().get (LCR_SCHEMA);
                            
                            assertTrue (
                                "Expecting DDL MetaData for: " + LCR_SCHEMA + ", " +
                                "got: " + ddl.getSchemataName(),
                                ddl.getSchemataName().equals (LCR_SCHEMA)
                            );
                            
                            assertTrue (
                                "Expecting " + NUM_COLUMNS + "columns, got: " +
                                ddl.getTableColumns().size(),
                                ddl.getTableColumns().size() == NUM_COLUMNS
                            );
                            
                            Column c = ddl.getTableColumns().get(0);
                            
                            assertTrue (
                                "Expecting column name: " + COLUMN_NAME +
                                " type: " + columnType + ", got: "      + 
                                c.toString(),
                                c.getName().equals (COLUMN_NAME) &&
                                c.getType().equals (columnType)
                           );
                        }
                        else {
                            /* drop DDL */
                            assertTrue (
                                "Expecting two cached PLOG schemas when " + 
                                "dropping: " + LCR_SCHEMA + " ,got: " + 
                                plog.getSchemas().size() + " schemas, " +
                                plog.getSchemas().keySet().toString(),
                                plog.getSchemas().size() == 2 &&
                                plog.getSchemas().containsKey(LCR_SCHEMA)
                            );
                            
                            plog.getSchemas().remove (LCR_SCHEMA);
                            
                            Object []keys = plog.getSchemas().keySet().toArray();
                            String drop = (String)keys[0];
                            
                            assertTrue (
                                "Expecting second DDL to be for DROP table: " +
                                LCR_SCHEMA,
                                drop.startsWith("SOE.BIN")
                            );
                        }
                    }
                    
                    if (lcr.getAction().equals (ChangeAction.LOB_WRITE)) {
                        /* for dumping out entry record raw data
                        System.out.print (
                            "int []" + columnType + " = "
                        );
                        int []data = 
                            records.get(i)
                                   .getEntryTags()
                                   .get(EntryTagType.TAG_LOBDATA)
                                   .get(0)
                                   .getRawData();
                        
                        System.out.print ("{ ");
                        for (int d = 0; d < data.length; d++) {
                            if (d > 0) {
                                System.out.print (", ");
                            }
                            System.out.print (data[d]);
                        }
                        System.out.println (" };");
                        */
                        
                        assertTrue (
                            "Expecting " + NUM_COLUMNS + " column values, got: " +
                            lcr.getColumnValues().size(),
                            lcr.getColumnValues().size() == NUM_COLUMNS
                        );
                        
                        ColumnValue cr = lcr.getColumnValues().get(0);
                        
                        assertTrue (
                            "Expecting column id: " + COLUMN_ID + " name: " + 
                            COLUMN_NAME + " value: " + columnValue + ", got: "+
                            cr.toString(),
                            cr.getId() == COLUMN_ID &&
                            cr.getName().equals(COLUMN_NAME) &&
                            (cr.getValue() instanceof SerialBlob 
                             ? cr.getValueAsString().equals(columnValue)
                             : cr.getValue().equals(columnValue))
                        );
                    }
                }
                
                plog.getSchemas().clear();
                plog.getDictionary().clear();
                
            }
            catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        }
}
