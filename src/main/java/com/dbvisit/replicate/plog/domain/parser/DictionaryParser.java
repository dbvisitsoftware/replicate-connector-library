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
import com.dbvisit.replicate.plog.metadata.ColumnState;
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
            rec.hasColumnMetaData()) 
        {
            if (plog.getDictionary() == null) {
                plog.setDictionary(new HashMap<Integer, Table>());
            }

            Map<Integer, Table> dictionary = plog.getDictionary();
            
            /* parse table meta data for row level dictionary */ 
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
                
                boolean merged = false;
                
                if (table.getColumns() != null && 
                    cache.getColumns() != null) 
                {
                    merged = mergeColumns (
                        table.getColumns(),
                        cache.getColumns()
                    );
                }
                
                if (merged) {
                    /* set table keys for existing schema after merging columns */
                    setTableKeys (plog, table);
                
                    /* overwrite existing one in cache */
                    dictionary.put (table.getId(), table);
                
                    logger.debug ("Updating dictionary: " + table.toString());
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
        
        if (!plog.getDictionary().containsKey(table.getId())) {
            /* set table keys from PLOG cache for unmodified schema */
            setTableKeys (plog, table);
        }

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
        
        if (rec.hasColumnMetaData()) {
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
     * Compare two list of columns and set the state of incoming columns
     * according to the comparison with old
     * 
     * @param current  The current list of columns decoded from LCR NOOP
     * @param previous The previous list of columns from dictionary cache
     * 
     * @return true if columns were merged, else false
     */
    public static boolean mergeColumns (
        List <Column> current,
        List <Column> previous   
    ) {
        boolean merged = false;
        
        /* should only be dealing with NOOP LCRs, this should not
         * be done frequently, first iterate through current
         * and compare, then add ones that were removed at end
         */
        for (int idx = 0; idx < current.size(); idx++) {
            Column c = current.get(idx);
            Column p = null;
            
            if (idx < previous.size()) {
                /* when current has added a new column at end */
                p = previous.get(idx);
            }
            if (c == null && p != null) {
                p.setState(ColumnState.REMOVED);
                /* removed, merge with previous */
                logger.trace (
                    "Column at idx: "   + idx + " removed " +
                    "column: " + p.toString()
                );
                current.set(idx, p);
                merged = true;
            }
            else if (c != null && p == null) {
                /* added new column, set state accordingly */
                c.setState(ColumnState.ADDED);
                logger.trace (
                    "Column at idx: "   + idx + " added " +
                    "column: " + c.toString()
                );
                merged = true;
            }
            else {
                /* both present */
                if (c.toCmpString().equals(p.toCmpString())) {
                    c.setState(ColumnState.UNCHANGED);
                }
                else {
                    logger.trace (
                        "Column at idx: "   + idx + " modified "  +
                        "column: " + c.toString() + " previous: " +
                        p.toString()
                    );
                    /* modified, set state accordingly */
                    c.setState(ColumnState.MODIFIED);
                    merged = true;
                }
            }
        }
        if (current.size() < previous.size()) {
            /* less columns than previously, these will be at end */
            int csize = current.size();
            
            for (int idx = csize; idx < previous.size(); idx++) {
                Column p = previous.get(idx);
                p.setState(ColumnState.REMOVED);
                logger.trace (
                    "Column at idx: "   + idx + " removed " +
                    "column: " + p.toString()
                );
                current.add (p);
                merged = true;
            }
        }
        
        return merged;
    } 
    
    /**
     * Attach the schema keys to table dictionary for use by column values
     * 
     * @param plog  current parsed PLOG file
     * @param table the already parsed table definition
     */
    private void setTableKeys (PlogFile plog, Table table) {
        if (plog.getSchemas().containsKey (table.getFullName())) {
            DDLMetaData md = plog.getSchemas().get (table.getFullName());
            List<Column> cols = md.getTableColumns();
            
            for (int c = 0; c < cols.size(); c++) {
                if (cols.get(c).isKey()) {
                    if (c < table.getColumns().size() &&
                        table.getColumns().get(c) != null)
                    {
                        logger.debug (
                            "Setting dictionary table: " + table.getFullName() + 
                            " key for column: " + 
                            table.getColumns().get(c).getName()
                        );
                        table.getColumns().get(c).setIsKey(true);
                    }
                }
            }
            
            /* set whether or not the table dictionary has key constraints */
            table.setHasKey(md.hasKey());
            
            logger.debug (
                "table: " + table.getFullName() + " has key: " + table.hasKey()
            );
        }
    }
    
}
