package com.dbvisit.replicate.plog.reader.criteria;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.dbvisit.replicate.plog.domain.DomainRecord;
import com.dbvisit.replicate.plog.domain.ReplicateOffset;
import com.dbvisit.replicate.plog.file.PlogFile;
import com.dbvisit.replicate.plog.format.EntrySubType;
import com.dbvisit.replicate.plog.reader.PlogStreamReader;

public class SchemaOffsetCriteriaTest extends CriteriaTest {
    @Test
    public void testSkipOnlyHeader() {
        logger.info ("Skipping only header, all data must be processed");
        
        /* see base class for details of data */
        try {
            /* filter only the header record */
            @SuppressWarnings("serial")
            SchemaOffsetCriteria<EntrySubType> parseCriteria =
                new SchemaOffsetCriteria<EntrySubType> (
                    new HashMap<String, ReplicateOffset>() {{
                        put (TABLE_1_NAME, new ReplicateOffset (0L, 112L));
                        put (TABLE_2_NAME, new ReplicateOffset (0L, 112L));
                        put (TABLE_3_NAME, new ReplicateOffset (0L, 112L));
                    }}
            );
            /* check counts */
            String [] TABLE_NAMES = { 
                TABLE_1_NAME, 
                TABLE_2_NAME, 
                TABLE_3_NAME
            };
            int [] TABLE_DATA_COUNT = {
                TABLE_1_DATA_COUNT,
                TABLE_2_DATA_COUNT,
                TABLE_3_DATA_COUNT
            };
            int [] TABLE_META_COUNT = {
                TABLE_1_META_COUNT,
                TABLE_2_META_COUNT,
                TABLE_3_META_COUNT
            };
            
            testSkipOffsetCriteria (
                parseCriteria,
                TABLE_NAMES,
                TABLE_DATA_COUNT,
                TABLE_META_COUNT
            );
        }
        catch (Exception e) {
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testSkipFirstTable() {
        logger.info ("Skipping data for table: " + TABLE_1_NAME);
        
        /* see base class for details of data */
        try {
            /* filter first table by offset */
            @SuppressWarnings("serial")
            SchemaOffsetCriteria<EntrySubType> parseCriteria =
                new SchemaOffsetCriteria<EntrySubType> (
                    new HashMap<String, ReplicateOffset>() {{
                        put (TABLE_1_NAME, new ReplicateOffset (0L, 99999999L));
                    }}
            );
            /* check counts */
            String [] TABLE_NAMES = { 
                TABLE_1_NAME, 
                TABLE_2_NAME, 
                TABLE_3_NAME
            };
            int [] TABLE_DATA_COUNT = {
                0,
                TABLE_2_DATA_COUNT,
                TABLE_3_DATA_COUNT
            };
            int [] TABLE_META_COUNT = {
                TABLE_1_META_COUNT,
                TABLE_2_META_COUNT,
                TABLE_3_META_COUNT
            };
            
            testSkipOffsetCriteria (
                parseCriteria,
                TABLE_NAMES,
                TABLE_DATA_COUNT,
                TABLE_META_COUNT
            );
        }
        catch (Exception e) {
            fail (e.getMessage());
        }
    }
    
    @Test
    public void testSkipFirstDataRecord() {
        logger.info ("Skipping first record of each data for table: " + TABLE_1_NAME);
        
        /* see base class for details of data */
        try {
            /* parse criteria, pre-filter each first record by offset */
            @SuppressWarnings("serial")
            SchemaOffsetCriteria<EntrySubType> parseCriteria =
                new SchemaOffsetCriteria<EntrySubType> (
                    new HashMap<String, ReplicateOffset>() {{
                        put (TABLE_1_NAME, new ReplicateOffset (0L, 5268L));
                        put (TABLE_2_NAME, new ReplicateOffset (0L, 6344L));
                        put (TABLE_3_NAME, new ReplicateOffset (0L, 7720L));
                    }}
            );
            /* check counts */
            String [] TABLE_NAMES = { 
                TABLE_1_NAME, 
                TABLE_2_NAME, 
                TABLE_3_NAME
            };
            /* expecting one less record for each table */
            int [] TABLE_DATA_COUNT = {
                TABLE_1_DATA_COUNT - 1,
                TABLE_2_DATA_COUNT - 1,
                TABLE_3_DATA_COUNT - 1
            };
            int [] TABLE_META_COUNT = {
                TABLE_1_META_COUNT,
                TABLE_2_META_COUNT,
                TABLE_3_META_COUNT
            };
            
            testSkipOffsetCriteria (
                parseCriteria,
                TABLE_NAMES,
                TABLE_DATA_COUNT,
                TABLE_META_COUNT
            );
        }
        catch (Exception e) {
            fail (e.getMessage());
        }
    }

    public void testSkipOffsetCriteria (
        SchemaOffsetCriteria<EntrySubType> parseCriteria,
        String [] expectedTables,
        int [] expectedDataCounts,
        int [] expectedMetaCounts
    ) {
        /* see base class for details of data */
        try {
            PlogStreamReader plogStream = openAndValidatePLOG(parseCriteria);
            PlogFile plog = plogStream.getPlog();
            
            /* flush every record */
            plogStream.setFlushSize(1);
            
            Map<String, Integer> tableDataCount = 
                new HashMap<String, Integer>();
            Map<String, Integer> metaDataCount = 
                new HashMap<String, Integer>();
            
            while (!plogStream.isDone()) {
                plogStream.read();
                
                if(plogStream.canFlush()) {
                    DomainRecord dr = plogStream.flush().get(0);
                    
                    String schema = dr.getRecordSchema();
                    
                    if (dr.isChangeRowRecord()) {
                        if (!tableDataCount.containsKey(schema)) {
                            tableDataCount.put (schema, 0);
                        }
                        
                        int count = tableDataCount.get (schema);
                        
                        tableDataCount.put (schema, ++count);
                    }
                    else if (dr.isMetaDataRecord()) {
                        if (!metaDataCount.containsKey(schema)) {
                            metaDataCount.put (schema, 0);
                        }
                        
                        int count = metaDataCount.get (schema);
                        
                        metaDataCount.put (schema, ++count);
                    }
                }
            }
            
            assertTrue (
                "Footer has been read, reader must be done",
                plogStream.isDone()
            );
            
            plog.close();
            
            logger.info (
                "Table data counts: " + 
                tableDataCount.toString().replace("{", "").replace("}", "")
            );
            
            logger.info (
                "Meta data counts: " + 
                metaDataCount.toString().replace("{", "").replace("}", "")
            );
            
            if (expectedTables.length != expectedDataCounts.length ||
                expectedTables.length != expectedMetaCounts.length) 
            {
                throw new Exception ("Invalid expected test arrays provided");
            }
            
            for (int i = 0; i < expectedTables.length; i++) {
                String tableName = expectedTables[i];
                int dataCount = expectedDataCounts[i];
                int metaCount = expectedMetaCounts[i];
                
                assertTrue (
                    "Expecting data for table: " + tableName,
                    tableDataCount.containsKey(tableName) ||
                    dataCount == 0
                );
                
                assertTrue (
                    "Expecting meta data for table: " + tableName,
                    metaDataCount.containsKey(tableName) ||
                    metaCount == 0
                );
                
                assertTrue (
                    tableName + " - expecting: " + dataCount + 
                    " data records, got: " +
                    tableDataCount.get(tableName),
                    dataCount == 0 ||
                    tableDataCount.get(tableName) == dataCount
                );

                assertTrue (
                    "Expecting: " + metaCount + " meta data records, got: " +
                    metaDataCount.get(tableName),
                    metaCount == 0 ||
                    metaDataCount.get(tableName) == metaCount
                );
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }

    @Override
    @Test
    public void testPLOGTableOffsets () {
        try {
            super.testPLOGTableOffsets();
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
}
