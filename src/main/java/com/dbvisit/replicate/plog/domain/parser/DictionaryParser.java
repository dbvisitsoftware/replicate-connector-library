package com.dbvisit.replicate.plog.domain.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.plog.file.PlogFile;
import com.dbvisit.replicate.plog.format.EntryRecord;
import com.dbvisit.replicate.plog.format.EntryTagRecord;
import com.dbvisit.replicate.plog.format.EntryTagType;
import com.dbvisit.replicate.plog.format.decoder.SimpleDataDecoder;
import com.dbvisit.replicate.plog.metadata.Column;
import com.dbvisit.replicate.plog.metadata.DDLMetaData;
import com.dbvisit.replicate.plog.metadata.Table;

/**
 * Copyright 2016 Dbvisit Software Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

/** Parses and updates replicated table dictionary for cache in PLOG file,
 *  this is not a domain parser. This is required by other domain parser
 *  for decoding column information encoded in compact form in PLOG
 */
public class DictionaryParser {
    private static final Logger logger = LoggerFactory.getLogger(
        DictionaryParser.class
    );
    
    /**
     * Read column meta data in and build dictionary cache for decoding
     * column data in compact PLOG. <p>If needed update the PLOG cache with 
     * either information from entry record or PLOG's schema definitions.</p>
     * 
     * @param plog Compact encoded PLOG to build dictionary for
     * @param rec  Parsed PLOG entry record
     * 
     * @throws Exception Failed to parse dictionary for compact PLOG
     */
    public void parse (PlogFile plog, EntryRecord rec) 
    throws Exception {
        if (plog.isCompact() &&
            rec != null && 
            rec.useColumnMetaData() && 
            rec.canParseColumnMetaData()) 
        {
            if (plog.getDictionary() == null) {
                plog.setDictionary(new HashMap<Integer, Table>());
            }

            Map<Integer, Table> dictionary = plog.getDictionary();
            
            boolean usedColumnMetaData = true;

            /* aim is to build dictionary for compact PLOG decoding.
             * decode meta data from entry record if it has any, if none
             * available we try a cache hit on schema definitions
             */
            Table table = getTableMetaData(plog, rec);
                
            logger.debug ("Dictionary parsed: " + table.toString());
                        
            /* dictionary is used for caching minimal meta data for
             * compact encoding of PLOG, not schema DDL
             */
            if (!dictionary.containsKey(table.getId())) {
                dictionary.put (table.getId(), table);
            }
            else {
                /* update dictionary columns */
                Table cache = dictionary.get(table.getId());
                
                /* check for update */
                if (table.getColumns() != null && 
                    cache.getColumns() != null) 
                {
                    updateColumnsWithMetaData (
                        cache.getColumns(),
                        table.getColumns()
                    );
                }
            }
            
            if (usedColumnMetaData) {
                updateSchemaFromMetaData (plog, rec, table.getColumns());
            }
        }
    }
    
    /** 
     * Update the schema definition from the parsed column meta data. This
     * is only done if needed as fallback for strange PLOGs.
     * 
     * @param plog     Parsed PLOG file
     * @param rec      Parsed PLOG entry record
     * @param metadata Parsed column meta data
     * 
     * @throws Exception Failed to update schema definition from PLOG entry
     */
    private void updateSchemaFromMetaData (
        PlogFile plog,
        EntryRecord rec,
        List<Column> metadata
    ) throws Exception 
    {
        if (metadata != null) {
            boolean updated = false;
            String schema   = rec.getRecordSchema();
            
            if (schema != null && plog.getSchemas().containsKey(schema)) {
                /* check if we need to update JSON schema definition 
                 * or rebuild column map 
                 */
                DDLMetaData schemaDDL = plog.getSchemas().get(schema);
                    
                if (updateColumnsWithMetaData (
                        schemaDDL.getTableColumns(),
                        metadata
                    )
                ) {
                    updated = true;
                        
                    /* update the SCN validity for schema definition */
                    schemaDDL.setValidSinceSCN (rec.getRecordSCN());
                        
                    plog.setUpdatedSchema(true);
                }
                            
                if (updated) {
                    Map <Integer, Column> colMap = 
                        plog.getSchemas().get(schema).getColumns();
                    List <Column> cols = 
                        plog.getSchemas().get(schema).getTableColumns();
                        
                    for (Column col : cols) {
                        if (!colMap.containsKey (col.getId())) {
                            colMap.put (col.getId(), col);
                        }
                    }
                }
            }
        }
    }
    
    /**
     * Parses table dictionary for compact PLOG
     * 
     * @param plog PLOG file with dictionary cache
     * @param rec PLOG entry record
     * 
     * @return Table dictionary meta data
     * @throws Exception if invalid record is parsed
     */
    private Table getTableMetaData (PlogFile plog, EntryRecord rec) 
    throws Exception {
        Table table = new Table();
        
        Map<EntryTagType, List<EntryTagRecord>> tags = rec.getEntryTags();
        EntryTagType type  = null;
        EntryTagRecord tag = null;

        type = EntryTagType.TAG_OBJ_ID;
        if (tags.containsKey(type)) {
            tag = tags.get (type).get(0);
            table.setId( 
                SimpleDataDecoder.decodeInteger(tag.getRawData())
            );
        }
        else {
            /* must have object ID */
            throw new Exception (
                "Invalid dictionary entry, no table object ID"
            );
        }
        
        type = EntryTagType.TAG_OBJ_OWNER;
        if (tags.containsKey(type)) {
            tag = tags.get (type).get(0);
            table.setOwner( 
                SimpleDataDecoder.decodeCharString(tag.getRawData())
            );
        }
        else if (plog.getDictionary().containsKey(table.getId())) {
            table.setOwner (
                plog.getDictionary().get(table.getId()).getOwner()
            );
        }
        else {
            logger.error (
                "Invalid PLOG entry, no table object owner for " +
                "table ID: " + table.getId() 
            );
        }

        type = EntryTagType.TAG_OBJ_NAME;
        if (tags.containsKey(type)) {
            tag = tags.get (type).get(0);
            table.setName(
                SimpleDataDecoder.decodeCharString(tag.getRawData())
            );
        }
        else if (plog.getDictionary().containsKey(table.getId())) {
            table.setName (
                plog.getDictionary().get(table.getId()).getName()
            );
        }
        else {
            logger.error (
                "Invalid PLOG entry, no table object name for " +
                "table ID: " + table.getId() 
            ); 
        }
        
        table.setColumns(getColumnsMetaData(rec));

        return table;
    }
    
    /**
     * Get column meta data from PLOG entry record
     * 
     * @param rec parsed PLOG Entry record
     * 
     * @return List of column meta data
     * @throws Exception Failed to parse column meta data from PLOG entry
     */
    private List<Column> getColumnsMetaData (EntryRecord rec) 
    throws Exception {
        List<Column> columns = null;
        
        if (rec.canParseColumnMetaData()) {
            Map <EntryTagType, List<EntryTagRecord>> tags = rec.getEntryTags();
            
            Map <Integer, Integer> idxToId = 
                new LinkedHashMap <Integer, Integer>();
        
            int numCols = 0;
            
            /* decode column ordinal numbers and set number of columns */
            int i = 0;
            for (EntryTagRecord tag : tags.get (EntryTagType.TAG_COL_ID)) {
                int id = SimpleDataDecoder.decodeInteger(tag.getRawData());
                
                if (numCols < id) {
                    numCols = id;
                }

                idxToId.put (i++, id);
            }
                
            columns = new ArrayList<Column>(numCols);
            
            while (columns.size() < numCols) {
                columns.add (null);
            }
        
            /* now parse the column meta tags */
            EntryTagType type;
            
            /* insert/update order is retained, single entry of column metadata 
             * each column ID is the ordinal number 
             */
            type = EntryTagType.TAG_COL_ID;
            for (EntryTagRecord tag : tags.get (type)) {
                int id = SimpleDataDecoder.decodeInteger(tag.getRawData());
                Column column = new Column();
                column.setId (id);

                /* id is ordinal number, convert it to index */
                int idx = Column.toColumnIdx(id);
                
                if (idx >= 0) {
                    columns.set(idx, column);
                }
                else {
                    logger.warn ("Skipping invalid column, ID: " + id);
                }
            }

            i = 0;
            type = EntryTagType.TAG_COL_NAME;
            for (EntryTagRecord tag : tags.get (type)) {
                int idx = Column.toColumnIdx (idxToId.get(i++));
                
                String colName =
                    SimpleDataDecoder.decodeCharString(tag.getRawData());
                
                if (idx >= 0) {
                    columns.get(idx).setName(colName);
                }
                else {
                    logger.warn ("Skipping invalid column, name: " + colName);
                }
            }
            
            i = 0;
            type = EntryTagType.TAG_COL_TYPE;
            for (EntryTagRecord tag : tags.get (type)) {
                int idx = Column.toColumnIdx (idxToId.get(i++));
                
                String colType = 
                    SimpleDataDecoder.decodeCharString(tag.getRawData());
                
                if (idx >= 0) {
                    columns.get(idx).setType(colType);
                }
                else {
                    logger.warn ("Skipping invalid column, type: " + colType);
                }
            }
        }
        
        return columns;
    }
    
    /**
     * Support DDL removing or adding column meta data only
     * 
     * @param columns  The current list of columns from dictionary
     * @param metadata The current list of columns from meta data
     * 
     * @return true if it's been updated, else false
     */
    private boolean updateColumnsWithMetaData (
        List <Column> columns,
        List <Column> metadata   
    ) throws Exception 
    {
        boolean updated = false;
        
        /* only support adding new columns, not modifying existing
         * columns. when columns are removed we set it to 
         * non-mandatory 
         */
        if (columns.size() > metadata.size()) {
            /* compare by ordinal number, fields removed */
            for (int c = metadata.size(); c < columns.size(); c++) {
                columns.get (c).setNullable(true);
                updated = true;
            }
            
            /* check that none of the existing columns have changed 
             * either name or type change is compatible, scale and
             * precision is tolerated */
            boolean incompatible = false;
            
            for (int c = 0; c < metadata.size(); c++) {
                Column c1 = metadata.get (c);
                
                if (c1 != null && c1.getName() != null) {
                    Column c2 = columns.get (c);
                    
                    if (c2 != null && 
                        !c1.toCmpString().equals (c2.toCmpString())) 
                    {
                        incompatible = true;
                    }
                }
            }
            
            if (incompatible) {
                throw new Exception (
                    "Unsupported DDL operation, reason existing columns " +
                    " have been modified"
                );
            }
        }
        else if (columns.size() < metadata.size()) {
            for (int c = columns.size(); c < metadata.size(); c++) {
                Column md = metadata.get (c);
                
                /* must not be mandatory */
                md.setNullable(true);
                
                columns.add (md);
                updated = true;
            }

            /* check that none of the existing columns have changed */
            boolean incompatible = false;
            
            for (int c = 0; c < metadata.size(); c++) {
                Column c1 = metadata.get (c);
                
                if (c1 != null && c1.getName() != null) {
                    Column c2 = columns.get (c);
                    
                    if (c2 != null && 
                        !c1.toCmpString().equals (c2.toCmpString())) 
                    {
                        incompatible = true;
                    }
                }
            }
            
            if (incompatible) {
                throw new Exception (
                    "Unsupported DDL operation, reason existing columns " +
                    "have been modified. " +
                    "\nDetails: " + 
                    "\n" + columns.toString()  + "\n" + 
                    "have been changed to: "   + 
                    "\n" + metadata.toString() + "\n"
                );
            }
        }
        
        return updated;
    }
    
}
