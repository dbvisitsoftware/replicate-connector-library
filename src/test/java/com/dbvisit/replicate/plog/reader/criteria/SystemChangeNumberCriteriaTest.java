package com.dbvisit.replicate.plog.reader.criteria;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;

import com.dbvisit.replicate.plog.domain.DomainRecord;
import com.dbvisit.replicate.plog.file.PlogFile;
import com.dbvisit.replicate.plog.format.EntrySubType;
import com.dbvisit.replicate.plog.reader.DomainReader;
import com.dbvisit.replicate.plog.reader.PlogStreamReader;

public class SystemChangeNumberCriteriaTest extends CriteriaTest {
    
    @Test
    public void testGlobalSCNFilterAll () {
        logger.info ("Filter all PLOG entries by global SCN");
        long GLOBAL_START_SCN = 99999999L;
        
        SystemChangeNumberCriteria<EntrySubType> parseCriteria = 
            new SystemChangeNumberCriteria<EntrySubType> (GLOBAL_START_SCN);
        
        testSCNCriteria (
            parseCriteria,
            new String [] {},
            new int[] {},
            new int[] {}
        );
    }
    
    @Test
    public void testGlobalSCNFilterFirstTwoTables () {
        logger.info ("Filter first two table's PLOG entries by global SCN");
        long GLOBAL_START_SCN = 475319L;
        
        SystemChangeNumberCriteria<EntrySubType> parseCriteria = 
            new SystemChangeNumberCriteria<EntrySubType> (GLOBAL_START_SCN);
        
        testSCNCriteria (
            parseCriteria,
            new String [] { TABLE_3_NAME },
            new int[] { TABLE_3_DATA_COUNT },
            new int[] { 
                TABLE_1_META_COUNT,
                TABLE_2_META_COUNT,
                TABLE_3_META_COUNT
            }
        );
    }
    
    @Test
    public void testRegexpSCNFilterAllTables () {
        logger.info ("Filter all SOE.* by SCN");
        @SuppressWarnings("serial")
        SystemChangeNumberCriteria<EntrySubType> parseCriteria = 
            new SystemChangeNumberCriteria<EntrySubType> (
                new LinkedHashMap<String, Long>() {{
                    put ("SOE.*", 99999999L);
                }}
        );
        
        testSCNCriteria (
            parseCriteria,
            new String [] {},
            new int[] {},
            new int[] {}
        );
    }
    
    @Test
    public void testRegexpSCNFilterRecords () {
        logger.info ("Filter SOE.* by SCN 475316");
        @SuppressWarnings("serial")
        SystemChangeNumberCriteria<EntrySubType> parseCriteria = 
            new SystemChangeNumberCriteria<EntrySubType> (
                new LinkedHashMap<String, Long>() {{
                    put ("*", 475316L);
                }}
        );
        
        testSCNCriteria (
            parseCriteria,
            new String [] {
                TABLE_1_NAME,
                TABLE_2_NAME,
                TABLE_3_NAME
            },
            new int[] {
                TABLE_1_DATA_COUNT - 1,
                TABLE_2_DATA_COUNT,
                TABLE_3_DATA_COUNT
            },
            new int[] {
                TABLE_1_META_COUNT,
                TABLE_2_META_COUNT,
                TABLE_3_META_COUNT 
            }
        );
    }

    @Test
    public void testFullNameSCNFilterRecords () {
        logger.info ("Filter SOE.INVENTORIES by SCN 475320");
        @SuppressWarnings("serial")
        SystemChangeNumberCriteria<EntrySubType> parseCriteria = 
            new SystemChangeNumberCriteria<EntrySubType> (
                new LinkedHashMap<String, Long>() {{
                    put ("SOE.INVENTORIES", 475320L);
                    put ("*", 0L);
                }}
        );
        
        testSCNCriteria (
            parseCriteria,
            new String [] {
                TABLE_1_NAME,
                TABLE_2_NAME
            },
            new int[] {
                TABLE_1_DATA_COUNT ,
                TABLE_2_DATA_COUNT
            },
            new int[] {
                TABLE_1_META_COUNT,
                TABLE_2_META_COUNT,
                TABLE_3_META_COUNT   
            }
        );
    }
    
    public void testSCNCriteria (
        SystemChangeNumberCriteria<EntrySubType> parseCriteria,
        String [] expectedTables,
        int [] expectedDataCounts,
        int [] expectedMetaCounts
    ) {
        /* see base class for details of data */
        try {
            PlogFile plog = openAndValidatePLOG();
            
            DomainReader domainReader = plog.getReader().getDomainReader();
           
            domainReader.setParseCriteria(parseCriteria);
            
            PlogStreamReader plogStream = plog.getReader();
            
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
                    
                    if (dr.isChangeRecord()) {
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
            
            if (!tableDataCount.isEmpty()) {
                logger.info (
                    "Table data counts: " + 
                    tableDataCount.toString().replace("{", "").replace("}", "")
                );
            }
            
            if (!metaDataCount.isEmpty()) {
                logger.info (
                    "Meta data counts: " + 
                    metaDataCount.toString().replace("{", "").replace("}", "")
                );
            }
            
            if (expectedTables.length != expectedDataCounts.length &&
                expectedTables.length != expectedMetaCounts.length) 
            {
                throw new Exception ("Invalid expected test arrays provided");
            }
            
            assertTrue (
                "Expecting: " + expectedDataCounts.length + " tables with " +
                "data records, got: " + tableDataCount.size(),
                expectedDataCounts.length == tableDataCount.size()
            );
            
            assertTrue (
                "Expecting: " + expectedMetaCounts.length + " tables with " +
                "meta data records, got: " + metaDataCount.size(),
                expectedMetaCounts.length == metaDataCount.size()
            );
               
            for (int i = 0; i < expectedTables.length; i++) {
                String tableName = expectedTables[i];
                int dataCount = expectedDataCounts[i];
                int metaCount = expectedMetaCounts[i];
                
                assertTrue (
                    "Expecting data for table: " + tableName,
                    tableDataCount.containsKey(tableName)
                );
                
                assertTrue (
                    "Expecting meta data for table: " + tableName,
                    metaDataCount.containsKey(tableName)
                );
                
                assertTrue (
                    tableName + " - expecting: " + dataCount + 
                    " data records, got: " +
                    tableDataCount.get(tableName),
                    tableDataCount.get(tableName) == dataCount
                );

                assertTrue (
                    "Expecting: " + metaCount + " meta data records, got: " +
                    metaDataCount.get(tableName),
                    metaDataCount.get(tableName) == metaCount
                );
            }
        }
        catch (Exception e) {
            fail (e.getMessage());
        }
    }

}
