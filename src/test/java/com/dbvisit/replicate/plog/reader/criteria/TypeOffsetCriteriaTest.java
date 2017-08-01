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

public class TypeOffsetCriteriaTest extends CriteriaTest {
    @Test
    public void testSkipAllDataByOffsetForFirstTable() {
        logger.info (
            "Skipping data records for table: " + TABLE_1_NAME + " " +
            "by excluding LCR data by offset: 5780"            
        );
        
        /* see base class for details of data */
        try {
            /* filter first table INSERT/UPDATE/DELETE records
             * by offset using raw record subtype in pre-parse 
             * filter
             */
            @SuppressWarnings("serial")
            TypeOffsetCriteria<EntrySubType> parseCriteria =
                new TypeOffsetCriteria<EntrySubType> (
                    new HashMap<EntrySubType, ReplicateOffset>() {{
                        put (
                            EntrySubType.ESTYPE_LCR_INSERT,
                            new ReplicateOffset (0L, 5780L)
                        );
                        put (
                            EntrySubType.ESTYPE_LCR_UPDATE,
                            new ReplicateOffset (0L, 5780L)
                        );
                        put (
                            EntrySubType.ESTYPE_LCR_DELETE, 
                            new ReplicateOffset (0L, 5780L)
                        );
                        put (
                            EntrySubType.ESTYPE_LCR_LOB_WRITE,
                            new ReplicateOffset (0L, 5780L)
                        );
                        /* white list, do not filter these, retain all */
                        put (
                            EntrySubType.ESTYPE_DDL_JSON, 
                            new ReplicateOffset (0L, 0L)
                        );
                        put (
                            EntrySubType.ESTYPE_LCR_NOOP,
                            new ReplicateOffset (0L, 0L)
                        );
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
            
            testTypeOffsetCriteria (
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
    
    public void testTypeOffsetCriteria (
        TypeOffsetCriteria<EntrySubType> parseCriteria,
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

}
