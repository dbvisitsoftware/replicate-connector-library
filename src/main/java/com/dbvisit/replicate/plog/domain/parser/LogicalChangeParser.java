package com.dbvisit.replicate.plog.domain.parser;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.plog.domain.ChangeAction;
import com.dbvisit.replicate.plog.domain.ColumnDataType;
import com.dbvisit.replicate.plog.domain.ColumnValue;
import com.dbvisit.replicate.plog.domain.DomainRecord;
import com.dbvisit.replicate.plog.domain.LogicalChangeRecord;
import com.dbvisit.replicate.plog.file.PlogFile;
import com.dbvisit.replicate.plog.format.EntryRecord;
import com.dbvisit.replicate.plog.format.EntryTagRecord;
import com.dbvisit.replicate.plog.format.EntryTagType;
import com.dbvisit.replicate.plog.format.decoder.ColumnDataDecoder;
import com.dbvisit.replicate.plog.format.decoder.DataDecoder;
import com.dbvisit.replicate.plog.format.decoder.LOBDataDecoder;
import com.dbvisit.replicate.plog.format.decoder.SimpleDataDecoder;
import com.dbvisit.replicate.plog.metadata.Column;
import com.dbvisit.replicate.plog.metadata.Table;

/**
 * Parses logical change record that represent the state of the source record
 * after the logical change have been applied, a snapshot record at point in
 * time
 */
public class LogicalChangeParser implements DomainParser {
    private static final Logger logger = LoggerFactory.getLogger(
        LogicalChangeParser.class
    );
    
    /** Current LCR being parsed */
    private LogicalChangeRecord lcr;
    
    /* Merge multi-part data LCRs */
    private boolean mergeMultiPartLCRs = false;
    
    /* Meta data parser */
    final MetaDataParser metaDataParser = new MetaDataParser();
    
    /* Dictionary parser */
    final DictionaryParser dictionaryParser = new DictionaryParser();
    
    /**
     * Set wether or not to merge multi-part LCRs, this is needed because
     * LOBs fields are encoded in separate LCR from column data LCR
     * 
     * @param mergeMultiPartLCRs whether or not multi-part LCRs should be
     *                           merged
     */
    public void setMergeMultiPartLCRs (boolean mergeMultiPartLCRs) {
        this.mergeMultiPartLCRs = mergeMultiPartLCRs;
    }

    /**
     * Return whether or not multi-part change records are merged
     * 
     * @return true if merged, else false
     */
    public boolean mergeMultiPartLCRs () {
        return this.mergeMultiPartLCRs;
    }

    /**
     * Parse a PLOG entry and convert it to a LCR
     * 
     * @param plog PLOG file
     * @param rec  PLOG entry record
     * 
     * @throws Exception Failed to convert a PLOG entry to LCR
     */
    @Override
    public void parse (PlogFile plog, EntryRecord rec) throws Exception {
        if ((plog == null || !plog.canUse())) {
            throw new Exception ("FATAL: Invalid PLOG file provided to parse");
        }
        
        createLCR (plog, rec);
        
        parseFields (plog, rec);

        if (rec.isJSONMetaData()) {
            parseDDLMetaDataJSON (plog, rec);
        }

        /* cache dictionary for compact PLOG */ 
        if (plog.isCompact()) {
            dictionaryParser.parse (plog, rec);
        }

        if (rec.hasColumnData() && lcr.hasTableOwner()) {
            parseColumnData (plog, rec);
            
            /* cache partial LCRs by their unique key, may be
             * better to use a transaction entry queue */
            LogicalChangeRecord partialLCR = 
                plog.getPartialRecords().get(lcr.getUniqueKey());
            
            /* if needed merge data from multi-part LCRs into one */
            if (mergeMultiPartLCRs && 
                partialLCR != null &&
                canMergeMultiPartLCR (plog, partialLCR, lcr))
            {
                mergeMultiPartLCR (partialLCR, lcr);
                
                /* no need for previous LCR anymore */
                plog.getPartialRecords().remove (
                    lcr.getUniqueKey()
                );
            }
            
            /* complete, unless partial LCR */
            lcr.setComplete(true);
            
            /* lastly do not emit first part of multi-part LCR */
            if (mergeMultiPartLCRs && rec.isLOB()) {
                /* set previous LCR before filtering multi-part */
                plog.getPartialRecords().put (
                    lcr.getUniqueKey(), 
                    lcr
                );
                
                /* partial LCRs are not complete */
                lcr.setComplete(false);
            }
        }
        else {
            /* no column data, only if we are parsing non-data LCRs */
            lcr.setComplete(true);
        }
    }
    /**
     * Emit the parsed, complete and valid change record to calle, all 
     * state is cleared when done
     * 
     * @return parsed and complete change domain record
     */
    @Override
    public DomainRecord emit() {
        LogicalChangeRecord result = null;
        
        if (lcr != null) {
            result = lcr;
            lcr = null;
        }
        return result;
    }

    /**
     * Check if the currently parsed change record is complete, valid and ready
     * to be send back to caller. This means it's valid and complete, as in 
     * merged for a multi-part LCR, or valid and not merged if domain parser
     * has been told to not merge
     * 
     * @return true if ready to emit, else false, keep parsing
     */
    @Override
    public boolean canEmit() {
        boolean emit = false;
        
        if (lcr != null && 
            lcr.isComplete() && 
            (!lcr.isMultiPart() || (lcr.isMultiPart() && !mergeMultiPartLCRs)))
        {
            emit = true;
        }
        
        return emit;
    }

    /**
     * The logical change domain parser supports merging multi-part records
     * where LOB fields are separate LCRs from non-LOB fields
     * 
     * @return true
     */
    @Override
    public boolean supportMultiPartMerging() {
        return true;
    }

    /**
     * Enable multi-part record merging for LCRs
     */
    @Override
    public void enableMultiPartMerging() throws Exception {
        setMergeMultiPartLCRs (true);
    }
    
    /**
     * Merges a multi-part LCR by copying relevant fields from previously
     * parsed LOB only LCR, in cache, to the current data record
     * counterpart for the LOB write
     * 
     * @param prev Previously parsed LOB LCR
     * @param curr Current parsed data LCR
     * 
     * @throws Exception when any merge error occurs
     */
    private void mergeMultiPartLCR (
        LogicalChangeRecord prev, 
        LogicalChangeRecord curr
    ) throws Exception 
    {
        if (prev.getTableId() != curr.getTableId() ||
            prev.getColumnValues().size() != curr.getColumnValues().size()) 
        {
            throw new Exception (
                "Unable to merge LCRs from different tables or with " + 
                "different column data. Previous LCR: " + prev.toJSONString() +
                " Current LCR: " + curr.toJSONString()
            );
        }

        /* merge column data from previous to current LCR where column 
         * data is NULL
         */
        List <ColumnValue> prevRecs = prev.getColumnValues();
        List <ColumnValue> currRecs = curr.getColumnValues();

        for (int c = 0; c < prevRecs.size(); c++) {
            ColumnValue prevRec = prevRecs.get (c);
            ColumnValue currRec = currRecs.get (c);

            if (currRec == null && prevRec != null) {
                /* current column record is null, merge in previous */
                currRecs.set (c, prevRec);
            }

            /* only consider merging when current record is not null and
             * has no value; and the prev record is not null and has
             * valid value
             */
            if (currRec != null && currRec.getValue() == null &&
                prevRec != null && prevRec.getValue() != null) 
            {
                if (!prevRec.getName().equals (currRec.getName()) ||
                    !prevRec.getType().equals (currRec.getType()))
                {
                    throw new Exception (
                        "Unable to merge column record: " + c + " from " +
                        "different columns. Previous column: " + 
                        prevRec.toString() + " Current column: " +
                        currRec.toString()
                    );
                }
                currRec.setValue (prevRec.getValue());
            }
        }
    }
    
    /**
     * Determine to see if an incoming multi-part LCR can be merged with 
     * it's partial counterpart in PLOG cache
     * 
     * @param plog       The parsed PLOG file
     * @param partialLCR The partial LCR, the start of the multi-part LCR
     * @param lcr        The current LCR we would like to merge
     * 
     * @return true if LCR can be merged, else false
     */
    private boolean canMergeMultiPartLCR (
        PlogFile plog,
        LogicalChangeRecord partialLCR,
        LogicalChangeRecord lcr
    ) {
        boolean merge = false;
        
        /* criteria to merge
         * - prevLCR is valid
         * - prevLCR is LOB WRITE
         * - current record is a data change record
         * - same PLOG
         * - same transaction
         * - same table
         *
         * NOTE: 
         *   multi-part changes are not necessarily consecutive due to
         *   way Oracle emits the changes
         */
        if (partialLCR != null &&
            partialLCR.getAction().equals(ChangeAction.LOB_WRITE) &&
            lcr.hasColumnData() &&
            lcr.getPlogId() == partialLCR.getPlogId() &&
            lcr.getTransactionId().equals (partialLCR.getTransactionId()) &&
            lcr.getTableId() == partialLCR.getTableId())
        {
            merge = true;
        }
        return merge;
    }

    /**
     * Create empty logical change record object
     * 
     * @param plog PLOG file data
     * @param rec  Parsed PLOG entry record
     * 
     * @throws Exception Failed to create LCR
     */
    private void createLCR (PlogFile plog, EntryRecord rec)
    throws Exception {
        try {
            lcr = new LogicalChangeRecord();
            
            lcr.setAction(ChangeAction.find (rec.getSubType()));
            lcr.setIsMultiPart(rec.isLOB());
            
            /* if present will be overwritten in readLCR */
            lcr.setPlogId(plog.getId());
        } catch (Exception e) {
            throw new Exception (
                "Failed to create LCR, reason: " + e.getMessage(),
                e
            );
        }
    }
    
    /**
     * Parse the core fields of a logical change record
     * 
     * @param plog PLOG file data
     * @param rec  Parsed PLOG entry record
     * 
     * @throws Exception Failed to convert core LCR fields
     */
    private void parseFields (PlogFile plog, EntryRecord rec) 
    throws Exception {
        try {
            Map <EntryTagType, List<EntryTagRecord>> tags = 
                rec.getEntryTags();
            
            EntryTagRecord tag;
            
            /* eagerly read the LCR properties from tags using decoders */
            EntryTagType type = EntryTagType.TAG_PLOGSEQ;
            if (tags.containsKey(type)) {
                tag = tags.get (type).get(0);
                lcr.setPlogId (
                    SimpleDataDecoder.decodeInteger (tag.getRawData())
                );
            }
            
            type = EntryTagType.TAG_LCR_ID;
            if (tags.containsKey(type)) {
                tag = tags.get (type).get(0);
                lcr.setId(
                    SimpleDataDecoder.decodeLong(tag.getRawData()) 
                    + (1000000000L * lcr.getPlogId())
                );
            }
            
            type = EntryTagType.TAG_XID;
            if (tags.containsKey(type)) {
                tag = tags.get (type).get(0);
                lcr.setTransactionId(
                    SimpleDataDecoder.decodeCharString(tag.getRawData())
                );
            }
            
            type = EntryTagType.TAG_SAVEPOINT_ID;
            if (tags.containsKey(type)) {
                tag = tags.get (type).get(0);
                lcr.setSavePointId(
                    SimpleDataDecoder.decodeLong (tag.getRawData())
                );
            }
            
            type = EntryTagType.TAG_SCN;
            if (tags.containsKey(type)) {
                tag = tags.get (type).get(0);
                lcr.setSystemChangeNumber(
                    SimpleDataDecoder.decodeLong (tag.getRawData())
                );
            }
            
            type = EntryTagType.TAG_DTIME;
            if (tags.containsKey(type)) {
                tag = tags.get (type).get(0);
                lcr.setTimestamp(
                    SimpleDataDecoder.decodeDate(tag.getRawData())
                );
            }
            
            /* all LCRs at least need basic table meta data */
            type = EntryTagType.TAG_OBJ_OWNER;
            if (tags.containsKey(type)) {
                tag = tags.get (type).get(0);
                lcr.setTableOwner(
                    SimpleDataDecoder.decodeCharString(tag.getRawData())
                );
            }
            
            type = EntryTagType.TAG_OBJ_NAME;
            if (tags.containsKey(type)) {
                tag = tags.get (type).get(0);
                lcr.setTableName(
                    SimpleDataDecoder.decodeCharString(tag.getRawData())
                );
            }
            
            type = EntryTagType.TAG_OBJ_ID;
            if (tags.containsKey(type)) {
                tag = tags.get (type).get(0);
                lcr.setTableId(
                    SimpleDataDecoder.decodeInteger(tag.getRawData())
                );
                
                if (rec.hasColumnData() && !lcr.hasTableOwner()) {
                    /* PLOG is compact, lookup table owner for LCRs to 
                     * persist. If it's not in PLOG dictionary it has 
                     * been filtered 
                     */
                    if (plog.getDictionary().containsKey (
                            lcr.getTableId()
                        )
                    ) {
                        lcr.setTableOwner(
                            plog.getDictionary().get (
                                lcr.getTableId()
                            ).getOwner()
                        );

                        lcr.setTableName(
                            plog.getDictionary().get (
                                lcr.getTableId()
                            ).getName()
                        );
                    }
                }
            }
        } catch (Exception e) {
            throw new Exception (
                "Failed to decode LCR properties, reason: " + e.getMessage(),
                e
            );
        }
    }
    
    /**
     * Use the meta data parser to parse and cache meta data entries
     * for the PLOG from current raw PLOG entry record
     * 
     * @param plog Current PLOG
     * @param rec  PLOG entry record that has dictionary encoded in JSON
     *             as pay load
     *             
     * @throws Exception if a meta data parse error occur, including
     *                   JSON de-serialization error
     */
    private void parseDDLMetaDataJSON (PlogFile plog, EntryRecord rec)
    throws Exception {
        synchronized (metaDataParser) {
            /* defer the work to meta data parser, if configured to read 
             * JSON meta data PLOG entry records */
            metaDataParser.parse(plog, rec);
            metaDataParser.emit();
        }
    }
    
    /**
     * Read column data for a LCR from raw PLOG entry tags merged as complete
     * change record, not a change set
     * 
     * @param plog The current PLOG being parsed
     * @param rec  The parsed PLOG entry record
     * 
     * @throws Exception Failed to parse column data from PLOG entry
     */
    private void parseColumnData (PlogFile plog, EntryRecord rec) 
    throws Exception {
        /* parse value tags */
        Map <EntryTagType, List<EntryTagRecord>> tags = rec.getEntryTags();
        
        /* tag loop counter */
        int t;
        
        /* tag type */
        EntryTagType type;
        
        if (!tags.containsKey (EntryTagType.TAG_COL_ID)) {
            /* this LCR contains no column data */
            return;
        }
        
        /* determine number of columns from meta data */
        int numCols = 0;
        
        /* use LCR table owner and name as meta data schema lookup */
        String schema = lcr.getSchemaIdentifier();
        
        /* use dictionary, if none use schema definition */
        if (plog.getDictionary().containsKey (lcr.getTableId())) {
            numCols = 
                plog.getDictionary().get (lcr.getTableId()).getColumns().size();
        }
        else if (plog.getSchemas().containsKey(schema)) {
            numCols = 
                plog.getSchemas().get (schema).getColumns().size();
        }
        else {
            /* cannot read the column values if replication schema JSON
             * was not present in PLOG
             */
            logger.error (
                "Cannot read column data for: " + schema + ". It has no " +
                "meta data in the replicated schema cache, reason: " +
                "JSON DDL not present in PLOG"
            );
            return;
        }

        /* column index mask */
        boolean[] columnIdxMask = new boolean[numCols];
        
        /* prepare column data value list */
        if (lcr.getColumnValues() == null) {
            lcr.setColumnValues(new ArrayList<ColumnValue>());
        }
        
        List<ColumnValue> columnValues = lcr.getColumnValues();
        
        int i = 0;
        /* initialise all columns, whether or not they have data */
        while (columnValues.size() < numCols) {
            columnValues.add (null);
            columnIdxMask[i++] = false;
        }
        
        /* convert tag counter to column index */
        Map <Integer, Integer> tagToColumnIdx =
            new LinkedHashMap <Integer, Integer>();
        
        /* parse all columns present handling PRE/POST/KEY images */
        t = 0;
        type = EntryTagType.TAG_COL_ID;
        
        for (EntryTagRecord tag : tags.get (type)) {
            ColumnValue cdr = new ColumnValue ();
            
            /* column id */
            int id = SimpleDataDecoder.decodeInteger(tag.getRawData());
            
            /* column index */
            int columnIdx = Column.toColumnIdx (id);
            
            if (columnIdx >= columnIdxMask.length) {
                /* a column index that's not present in cached meta data
                 * this means that it was not emitted in PLOG by replicate
                 */
                logger.error (
                    "Column with ordinal number: " + id + " is not present "  +
                    "in the dictionary cache, it was not emitted correctly " + 
                    "and will be ignored"
                );
                
                /* re-allocate, add one extra column */
                boolean[] tmpMask = new boolean[id];
                for (int tmp = 0; tmp < tmpMask.length; tmp++) {
                    tmpMask[tmp] = false;
                    
                    if (tmp < columnIdxMask.length) {
                        tmpMask[tmp] = columnIdxMask[tmp];
                    }
                }
                columnIdxMask = tmpMask;
            }
            
            /* handle PRE/POST/KEY images, prepare column value only once */
            if (columnIdx < numCols && !columnIdxMask[columnIdx]) {
                /* column meta data is sorted by ordinal number, however 
                 * array index is zero based and ordinal number is 1 based,
                 * map meta data column index to lcr's column value index
                 */
                columnIdxMask[columnIdx] = true;

                cdr.setId(id);
                columnValues.set (columnIdx, cdr);
            }
            
            /* it's order corresponds to internal order of tags per record */
            tagToColumnIdx.put (t, columnIdx);
            t++;
        }
       
        if (plog.isCompact()) {
            /* read column meta data from PLOG cache */
            Table table = plog.getDictionary().get(lcr.getTableId());
            
            if (table == null) {
                throw new Exception (
                    "PLOG: " + plog.getFileName() + " has compactly encoded " +
                    "metadata, but no dictionary found for table: " +
                    lcr.getTableName()
                );
            }
            
            /* local column index counter for looking up meta data */
            int c = 0;
            for (Column column : table.getColumns()) {
                if (columnIdxMask[c]) {
                    /* attach meta data */
                    ColumnValue cdr = columnValues.get (c);
                    cdr.setName (column.getName());
                    cdr.setType (ColumnDataType.UNKNOWN.find(column.getType()));    
                }
                c++;
            }
        }
        else {
            /* column meta data is present in tags */
            int check = 0;
            /* tag loop counter */
            t = 0;
            /* tag type to parse */
            type = EntryTagType.TAG_COL_NAME;
            
            for (EntryTagRecord tag : tags.get (type)) {
                /* only retain tag loop needed */
                if (tagToColumnIdx.containsKey (t)) {
                    int idx = tagToColumnIdx.get (t);
                    columnValues.get(idx).setName(
                        SimpleDataDecoder.decodeCharString(tag.getRawData())
                    );
                    check++;
                }
                t++;
            }
            if (check != columnValues.size()) {
                throw new Exception (
                    "Column name metadata mismatch for " + type + " " +
                    check + "!=" + columnValues.size()
                );
            }
            
            /* check counter */
            check = 0;
            /* tag loop counter */
            t = 0;
            /* tag type to parse */
            type = EntryTagType.TAG_COL_TYPE;
            
            for (EntryTagRecord tag : tags.get (type)) {
                /* only retain tag loop needed */
                if (tagToColumnIdx.containsKey (t)) {
                    int idx = tagToColumnIdx.get (t);
                    String typeStr = 
                        SimpleDataDecoder.decodeCharString(tag.getRawData());
                
                    columnValues.get(idx).setType(
                        ColumnDataType.UNKNOWN.find(typeStr)
                    );
                    check++;
                }
                t++;
            }
            if (check != columnValues.size()) {
                throw new Exception (
                    "Column type metadata mismatch for " + type + " " +
                    check + "!=" + columnValues.size()
                );
            }
        }
        
        /* now decode data values in their encoded order for PRE/POST/KEY/LOB */
        List<EntryTagRecord> recs = null;
        
        /* supplemental key columns are sequential */
        int numKeys = 0;

        /* build the list of tags to go through in the correct order */
        recs = new LinkedList<EntryTagRecord>();
        List<List<EntryTagRecord>> datasets = 
            new ArrayList<List<EntryTagRecord>>();
        
        datasets.add(tags.get (EntryTagType.TAG_KEYIMAGE));
        datasets.add(tags.get (EntryTagType.TAG_PREIMAGE));
        datasets.add(tags.get (EntryTagType.TAG_POSTIMAGE));
        datasets.add(tags.get (EntryTagType.TAG_LOBDATA));
        
        for (List<EntryTagRecord> dataset : datasets) {
            if (dataset != null) {
                recs.addAll (dataset);
            }
        }
        
        if (datasets.get(0) != null) {
            /* set number of keys logged */
            numKeys = datasets.get(0).size();
        }
        
        /* sort by encoded order */
        Collections.sort (recs);
        
        /* iterate through sequence of tags and process */
        for (t = 0; t < recs.size(); t++) {
            EntryTagRecord tag = recs.get (t);
            
            if (!tagToColumnIdx.containsKey (t)) {
                throw new Exception (
                    "No column index found for data tag: " + t
                );
            }

            /* lookup the column index for this tag */
            int idx = tagToColumnIdx.get (t);
            
            if (!columnIdxMask[idx]) {
                /* skip this column, it's not needed */
                continue;
            }
                
            ColumnValue cdr = columnValues.get (idx);
            
            /* tags are in correct order, set which ones are part of key */
            if (t < numKeys) {
                cdr.setIsKey(true);
            }

            parseColumnValue (plog, rec, tag, cdr);
        }
        
        recs.clear();
        recs = null;
    }
    
    /**
     * Parse a column value by decoding the raw Oracle data type encoded in
     * PLOG entry record to domain types
     * 
     * @param plog The current PLOG file with cache to assist in decoding
     *             raw fields
     * @param rec  The raw PLOG entry record with Oracle value as payload
     * @param tag  The actual tag containing value to decode
     * @param columnValue a prepared column value
     * 
     * @throws Exception if column value decode error occurs
     */
    protected void parseColumnValue (
        PlogFile plog,
        EntryRecord rec,
        EntryTagRecord tag,
        ColumnValue columnValue
    ) throws Exception
    {
        Column metadata = null;
        
        String schema = lcr.getSchemaIdentifier();
        
        Map <EntryTagType, List<EntryTagRecord>> tags = rec.getEntryTags();

        if (plog.getSchemas().containsKey(schema)) {
            int colId = columnValue.getId();
            metadata =
                plog.getSchemas().get(schema).getColumns().get(colId);

            if (metadata == null) {
                throw new Exception (
                    "No column definition found for " + schema +
                    " column ID: " + colId
                );
            }
        }
        else {
            throw new Exception (
                "No schema definition found for: " + schema        
            );
        }
            
        switch (columnValue.getType ()) {
            case NUMBER:
            {
                int scale = metadata.getScale();
                if (
                    scale > 0 ||
                    metadata.getPrecision() <= 0 ||
                    metadata.getPrecision() - scale > 
                        DataDecoder.NUMBER_LONG_MAX_PRECISION
                ) {
                    columnValue.setValue (
                        ColumnDataDecoder.decodeNumber (
                            tag.getRawData(),
                            scale
                        )
                    );
                }
                else if (
                    scale <= 0 && 
                    metadata.getPrecision() - Math.abs(scale) < 
                        DataDecoder.NUMBER_INTEGER_MAX_PRECISION
                ) {
                    columnValue.setValue (
                        ColumnDataDecoder.decodeNumberAsInt (
                            tag.getRawData(),
                            scale
                        )
                    );
                }
                else {
                    columnValue.setValue (
                        ColumnDataDecoder.decodeNumberAsLong (
                            tag.getRawData(),
                            scale
                        )
                    );
                }
                break;
            }
            case VARCHAR2:
            case VARCHAR:
            case CHAR:
            case LONG:
            {
                columnValue.setValue(
                    ColumnDataDecoder.decodeString(tag.getRawData())
                );
                break;
            }   
            case NVARCHAR2:
            case NVARCHAR:
            case NCHAR:
            {
                columnValue.setValue(
                    ColumnDataDecoder.decodeNationalString(tag.getRawData())
                );
                break;
            }
            case RAW:
            case LONG_RAW:
            {
                columnValue.setValue (
                    ColumnDataDecoder.decodeBinary(tag.getRawData())
                );
                break;
            }
            case CLOB:
            case NCLOB:
            case CLOB_UTF16:
            {
                /* decode additional fields required by cLOB */
                if (tags.containsKey (EntryTagType.TAG_LOBLEN)) {
                    EntryTagRecord tr = 
                        tags.get (EntryTagType.TAG_LOBLEN).get (0);
                    
                    /* decode CLOB length in bytes */
                    columnValue.setLobLength (
                        SimpleDataDecoder.decodeLong(tr.getRawData())
                    );
                }
                
                columnValue.setValue(
                    LOBDataDecoder.decodeCLOB(
                        tag.getRawData(),
                        columnValue.getLobLength()
                    )
                );
                
                break;
            }
            case BLOB:
            {
                /* decode additional fields required by BLOB */
                if (tags.containsKey (EntryTagType.TAG_LOBLEN)) {
                    EntryTagRecord tr = 
                        tags.get (EntryTagType.TAG_LOBLEN).get (0);
                    
                    /* decode lob length */
                    columnValue.setLobLength (
                        SimpleDataDecoder.decodeLong(tr.getRawData())
                    );
                }
                
                if (tags.containsKey (EntryTagType.TAG_LOB_POSITION)) {
                    EntryTagRecord tr = 
                        tags.get (EntryTagType.TAG_LOB_POSITION).get (0);
                    
                    /* decode lob position */
                    columnValue.setLobPosition (
                        SimpleDataDecoder.decodeInteger(tr.getRawData())
                    );
                    
                }
                
                if (tags.containsKey (EntryTagType.TAG_LOBOFFSET)) {
                    EntryTagRecord tr = 
                        tags.get (EntryTagType.TAG_LOBOFFSET).get (0);
                    
                    /* decode lob offset */
                    columnValue.setLobOffset (
                        SimpleDataDecoder.decodeLong(tr.getRawData())
                    );
                }
                
                /* support have simple LOBs decode it as binary */
                columnValue.setValue (
                    LOBDataDecoder.decodeBLOB(
                        tag.getRawData(),
                        columnValue.getLobLength()
                    )
                );
                break;
            }
            case DATE:
            {
                columnValue.setValue (
                    ColumnDataDecoder.decodeDate(tag.getRawData())        
                );
                break;
            }
            case TIMESTAMP:
            {
                columnValue.setValue (
                    ColumnDataDecoder.decodeTimestamp(tag.getRawData())    
                );
                break;
            }
            case TIMESTAMP_WITH_TIME_ZONE:
            {
                columnValue.setValue (
                    ColumnDataDecoder.decodeTimestampWithTz(
                        tag.getRawData()
                    )    
                );
                break;
            }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            {
                columnValue.setValue (
                    ColumnDataDecoder.decodeTimestampWithLocalTz(
                        tag.getRawData()
                    )    
                );
                break;
            }
            case INTERVAL_DAY_TO_SECOND:
            {
                columnValue.setValue (
                    ColumnDataDecoder.decodeIntervalDayToSec(
                        tag.getRawData()
                    )
                );
               break;
            }
            case INTERVAL_YEAR_TO_MONTH:
            {
                columnValue.setValue (
                    ColumnDataDecoder.decodeIntervalYearToMonth(
                        tag.getRawData()
                    )
                );
                break;
            }
            case UNKNOWN:
            default:
                throw new Exception (
                    "Invalid column type: " + columnValue.getType()
                );
        }
    }
    
    /**
     * Return false, this is not an aggregate parser
     * 
     * @return false, change records are not aggregates
     */
    @Override
    public boolean isAggregateParser() {
        return false;
    }
    
}
