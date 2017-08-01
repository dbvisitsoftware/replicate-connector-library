package com.dbvisit.replicate.plog.domain.parser;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.plog.domain.ChangeAction;
import com.dbvisit.replicate.plog.domain.ChangeSetRecord;
import com.dbvisit.replicate.plog.domain.ColumnDataType;
import com.dbvisit.replicate.plog.domain.ColumnValue;
import com.dbvisit.replicate.plog.domain.DomainRecord;
import com.dbvisit.replicate.plog.domain.ChangeRowRecord;
import com.dbvisit.replicate.plog.file.PlogFile;
import com.dbvisit.replicate.plog.format.EntryRecord;
import com.dbvisit.replicate.plog.format.EntryTagRecord;
import com.dbvisit.replicate.plog.format.EntryTagType;
import com.dbvisit.replicate.plog.format.decoder.SimpleDataDecoder;
import com.dbvisit.replicate.plog.metadata.Column;
import com.dbvisit.replicate.plog.metadata.DDLMetaData;
import com.dbvisit.replicate.plog.metadata.Table;

public class ChangeSetParser extends ChangeRowParser {
    private static final Logger logger = LoggerFactory.getLogger(
        ChangeSetParser.class
    );
        
    /** current parsed change set record */
    protected ChangeSetRecord csr;
    
    /* Dictionary parser */
    final DictionaryParser dictionaryParser = new DictionaryParser();
    
    @Override
    public void parse (PlogFile plog, EntryRecord rec) throws Exception {
        if ((plog == null || !plog.canUse())) {
            throw new Exception ("Invalid PLOG file provided to parse");
        }
        
        createChangeSetRecord (plog, rec);
        
        /* set base class LCR as CSR to let parent parser populate the
         * core LCR fields
         */
        lcr = (ChangeRowRecord)csr;
        
        parseFields (plog, rec);

        if (rec.isJSONMetaData()) {
            parseDDLMetaDataJSON (plog, rec);
        }

        /* cache dictionary for compact PLOG */ 
        if (plog.isCompact()) {
            dictionaryParser.parse (plog, rec);
        }

        if (rec.hasColumnData() && csr.hasTableOwner()) {
            parseColumnData (plog, rec);
        }
    }

    @Override
    public DomainRecord emit() {
        ChangeSetRecord result = null;
        
        if (csr != null) {
            result = csr;
            csr = null;
        }
        return result;
    }

    @Override
    public boolean canEmit() {
        boolean emit = false;
        
        if (csr != null) {
            emit = true;
        }
        
        return emit;
    }
    
    private void createChangeSetRecord (PlogFile plog, EntryRecord rec)
    throws Exception {
        try {
            csr = new ChangeSetRecord();
                    
            csr.setAction(ChangeAction.find (rec.getSubType()));
            csr.setIsMultiPart(rec.isLOB());
                    
            /* if present will be overwritten */
            csr.setPlogId(plog.getId());
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                e.printStackTrace();
            }
            throw new Exception (
               "Failed to create LCR, reason: " + e.getMessage()
            );
        }
    }

    @Override
    public boolean supportMultiPartMerging() {
        return false;
    }
    
    @Override
    protected void parseColumnData (PlogFile plog, EntryRecord rec) 
    throws Exception {
        LinkedList<ColumnValue> columnValues = prepareColumnValues (plog, rec);
        
        Map <EntryTagType, List<EntryTagRecord>> tags = rec.getEntryTags();
        List<EntryTagRecord> recs = new LinkedList<EntryTagRecord>();
        
        /* need to parse in correct tag sequence */
        if (tags.containsKey(EntryTagType.TAG_KEYIMAGE)) {
            recs.addAll (tags.get(EntryTagType.TAG_KEYIMAGE));
        }
        if (tags.containsKey(EntryTagType.TAG_PREIMAGE)) {
            recs.addAll (tags.get(EntryTagType.TAG_PREIMAGE));
        }
        if (tags.containsKey(EntryTagType.TAG_POSTIMAGE)) {
            recs.addAll (tags.get(EntryTagType.TAG_POSTIMAGE));
        }
        if (tags.containsKey(EntryTagType.TAG_LOBDATA)) {
            recs.addAll (tags.get(EntryTagType.TAG_LOBDATA));
        }
        
        Collections.sort (recs);
        
        for (EntryTagRecord tag :recs) {
            switch (EntryTagType.TAG_UNKNOWN.find (tag.getId())) {
                case TAG_KEYIMAGE:
                {
                    /* populate value for key and pop it from 
                     * incoming value list */
                    ColumnValue keyValue = columnValues.pop();
                    /* this is part of KEY image */
                    keyValue.setIsSupLogKey(true);
                    
                    /* this is a fall back for PLOGs with a JSON meta data format
                     * that has no column key information
                     */
                    String schema = csr.getRecordSchema();
                    if (plog.getSchemas().containsKey (schema)) {
                        DDLMetaData md = plog.getSchemas().get (schema);
                        
                        if (!md.hasKey()) {
                            /* we have key image, apply it, this is not an INSERT */
                            Column col =
                                md.getColumns().get(keyValue.getId());
                            /* check schema meta data for tag */
                            if (!col.isKey()) {
                                logger.debug (
                                    "Older PLOG JSON format found - setting " +
                                    "missing key for column: " + col.getName()
                                );
                                /* use suplog key for older JSON format */
                                col.setIsKey(true);
                            }
                        }
                    }
                    
                    parseColumnValue(plog, rec, tag, keyValue);
                            
                    csr.addKeyValue(keyValue);
                            
                    logger.debug (
                        "key tag seq: " + tag.getSequence() + " " + 
                        "key value: " + keyValue.toString()
                    );
                    
                    break;
                }
                case TAG_PREIMAGE:
                {
                    /* populate value for pre/old and pop it from 
                     * incoming value list */
                    ColumnValue oldValue = columnValues.pop();
                    parseColumnValue(plog, rec, tag, oldValue);
                    
                    csr.addOldValue(oldValue);
                    
                    logger.debug (
                        "old tag seq: " + tag.getSequence() + " " + 
                        "old value: " + oldValue.toString()
                    );
                    
                    break;
                }
                case TAG_POSTIMAGE:
                {
                    /* populate value for post/new and pop it off 
                     * incoming value list */
                    ColumnValue newValue = columnValues.pop();
                    parseColumnValue(plog, rec, tag, newValue);
                    
                    csr.addNewValue(newValue);
                    
                    logger.debug (
                        "new tag seq: " + tag.getSequence() + " " + 
                        "new value: " + newValue.toString()
                    );
                    
                    break;
                }
                case TAG_LOBDATA:
                {
                    /* populate value for LOB (as post) and pop it off 
                     * incoming value list */
                    ColumnValue lobValue = columnValues.pop();
                    parseColumnValue(plog, rec, tag, lobValue);
                    
                    csr.addLobValue(lobValue);
                    
                    logger.debug (
                        "lob tag seq: " + tag.getSequence() + " " + 
                        "lob value: " + lobValue.toString() + " " +
                        "lob length: " + lobValue.getLobLength() + " " +
                        "lob offset: " + lobValue.getLobOffset() + " " +
                        "lob position: " + lobValue.getLobPosition()
                    );
                    
                    break;
                }
                default:
                    logger.error ("Unknown tag ID: " + tag.getId());
                    break;
            }
        }
    }
    
    /**
     * Prepares the CDC column values by initialising the meta data
     * part of each to allow decoding of values 
     * 
     * @param plog The parent PLOG with cache to use
     * @param rec  The parent PLOG entry record
     * 
     * @return List of prepared ColumnValue in encoded order
     */
    private LinkedList<ColumnValue> prepareColumnValues (
        PlogFile plog,
        EntryRecord rec
    ) throws Exception 
    {
        LinkedList<ColumnValue> columnValues = new LinkedList<ColumnValue>();
        
        Map <EntryTagType, List<EntryTagRecord>> tags = rec.getEntryTags();
        
        Collections.sort(tags.get (EntryTagType.TAG_COL_ID));
        int i = 0;
        for (EntryTagRecord tag : tags.get (EntryTagType.TAG_COL_ID)) {
            columnValues.add (new ColumnValue());
            columnValues.get(i).setId(
                SimpleDataDecoder.decodeInteger(tag.getRawData())
            );
            i++;
        }
        
        if (plog.isCompact()) {
            /* read column meta data from PLOG cache */
            Table table = plog.getDictionary().get(lcr.getTableId());
            
            if (table == null) {
                throw new Exception (
                    "PLOG: " + plog.getId() + " has compactly encoded " +
                    "metadata, but no dictionary found for table: " +
                    lcr.getTableName()
                );
            }

            for (ColumnValue cv : columnValues) {
                int c = Column.toColumnIdx(cv.getId());
                Column column = 
                    table.getColumns().get(c);
                
                if (column == null) {
                    throw new Exception (
                        "No column definition found for ordinal column: " +
                        cv.getId()
                    );
                }
                cv.setName (column.getName());
                cv.setType (ColumnDataType.UNKNOWN.find(column.getType()));
                
                /* dictionary table and column key are from constraints */
                if (table.hasKey()) {
                    /* set key as is, use key constraints */
                    cv.setIsKeyValue (column.isKey());
                }
                else if (
                    !table.hasKey() && 
                    !column.isKey() &&
                    !tags.containsKey (EntryTagType.TAG_KEYIMAGE) &&
                    column.canUseAsSuplogKey()
                ) {
                    logger.debug (
                        "Setting key for column: " + column.getName()  +
                        " action: " + lcr.getAction() + " for table: " +
                        table.getFullName() + " without a key definition"
                    );
                    /* there is no key constraints or key images (either
                     * from constraint or suplog) but this column value
                     * can be treated as key, this is a fall back
                     */
                    cv.setIsKeyValue(true);
                    
                    /* force it to be treated as if it was a key */
                    column.setIsKey(true);
                    
                    String schema = table.getFullName();
                    if (plog.getSchemas().containsKey (schema)) {
                        DDLMetaData md = plog.getSchemas().get (schema);
                        
                        if (!md.hasKey()) {
                            Column col = md.getTableColumns().get(c);
                            if (!col.isKey()) {
                                col.setIsKey(true);
                            }
                        }
                    }
                }
            }
        }
        else {
            Collections.sort(tags.get (EntryTagType.TAG_COL_NAME));
            i = 0;
            for (EntryTagRecord tag : tags.get (EntryTagType.TAG_COL_NAME)) {
                columnValues.add (new ColumnValue());
                columnValues.get(i).setName(
                    SimpleDataDecoder.decodeCharString(tag.getRawData())
                );
                /* set key column flag from cache if present */
                if (plog.getDictionary().containsKey(lcr.getTableId())) {
                    columnValues.get(i).setIsKeyValue(
                        plog.getDictionary()
                            .get(lcr.getTableId())
                            .getColumns()
                            .get(i)
                            .isKey()
                    );
                }
                i++;
            }
            
            Collections.sort(tags.get (EntryTagType.TAG_COL_TYPE));
            i = 0;
            for (EntryTagRecord tag : tags.get (EntryTagType.TAG_COL_TYPE)) {
                columnValues.add (new ColumnValue());
                columnValues.get(i).setType (
                    ColumnDataType.UNKNOWN.find (
                        SimpleDataDecoder.decodeCharString(tag.getRawData())
                    )
                );
                i++;
            }
        }
        
        return columnValues;
    }
}
