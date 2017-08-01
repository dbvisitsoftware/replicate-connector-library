package com.dbvisit.replicate.plog.domain.parser;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.plog.domain.TransactionInfoRecord;
import com.dbvisit.replicate.plog.file.PlogFile;
import com.dbvisit.replicate.plog.format.EntryRecord;
import com.dbvisit.replicate.plog.metadata.Table;

public class TransactionInfoParserTest extends ChangeParserTestConfig {
    private static final Logger logger = LoggerFactory.getLogger(
        TransactionInfoParserTest.class
    );
    
    private final int INSERT_NUM_ENTRY_RECORDS  = 6;
    
    /* the test fixtures contains the following PLOG entries:
     * 1. JSON DDL for CREATE TABLE
     * 2. NOOP for CREATE TABLE
     * 3. NOOP for INSERT
     * 4. INSERT
     * 5. JSON DDL for DROP TABLE</li>
     * 6. NOOP for DROP TABLE</li>
     */
    private final String[] EXPECTED_TX_IDS = {
        null,
        null,
        null,
        null,
        "0002.010.0000019e",
        null
    };
    
    private final int TABLE_ID = 20160;
    private final String TABLE_OWNER = "SOE";
    private final String TABLE_NAME = "UNITTEST";
    
    @Test
    public void testParseInsertLCRSet() {
        try {
            /* dummy plog with NULL domain reader */
            plog = new PlogFile(null) {
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
                public int getId() {
                    return 1;
                }
                @SuppressWarnings("serial")
                public Map<Integer, Table> getDictionary () {
                    final Table t = new Table();
                    t.setId(TABLE_ID);
                    t.setOwner(TABLE_OWNER);
                    t.setName(TABLE_NAME);
                    return new HashMap <Integer, Table>() {{
                        put (TABLE_ID, t);    
                    }};
                }
            };
            
            List<EntryRecord> records = parseEntryRecord (integerValueLCR());
            
            assertTrue (
                "Expecting " + INSERT_NUM_ENTRY_RECORDS + " entry records, " +
                "got: " + records.size(),
                records.size() == INSERT_NUM_ENTRY_RECORDS
            );
                
            TransactionInfoParser txp = new TransactionInfoParser();
                
            for (int i = 0; i < INSERT_NUM_ENTRY_RECORDS; i++) {
                txp.parse(plog, records.get(i));
                
                TransactionInfoRecord txr = (TransactionInfoRecord)txp.emit();

                if (EXPECTED_TX_IDS[i] == null) {
                    /* expect NULL */
                    assertNull(
                        "Expecting NO emited transacion info record",
                        txr
                    );
                }
                else {
                    logger.info (txr.toString());
                    assertTrue (
                        "Expecting previous TX: " + EXPECTED_TX_IDS[i] +
                        ", got: " + txr != null ? txr.getId() : "none", 
                        txr != null &&
                        txr.getId().equals (EXPECTED_TX_IDS[i])
                    );
                    
                    logger.info ("[" + i + "]: " + txr.toJSONString());
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

}
