package com.dbvisit.replicate.plog.domain;

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

import java.sql.Timestamp;
import java.util.List;

import com.dbvisit.replicate.plog.domain.util.DomainJSONConverter;
import com.fasterxml.jackson.annotation.JsonIgnore;

/** 
 * Logical domain abstraction for a change record, that is a record that
 * represents the state of a record after change was applied
 */
public class ChangeRowRecord extends DomainRecord {
    /** Type of change */
    private ChangeAction action;
    
    /** Unique id of LCR */
    private long id;
    
    /** PLOG ID */
    private int plogId;
    
    /** Oracle transaction ID */
    private String transactionId;
    
    /** LCR id where to roll back, not used in pessimistic commit */
    private long savePointId;
    
    /** Oracle SCN */
    private long systemChangeNumber;
    
    /** Time of change */
    private Timestamp timestamp;
    
    /** Parent table id of change record */
    private int tableId;
    
    /** Parent table owner of change record */
    private String tableOwner;
    
    /** Parent table name of change record */
    private String tableName;
    
    /** The column values for change record */
    private List<ColumnValue> columnValues;
    
    /** Whether or not this is part of a multi-part change record */
    private boolean isMultiPart = false;
    
    /**
     * Create empty logical change record with correct domain type
     */
    public ChangeRowRecord () {
        recordType = DomainRecordType.CHANGEROW_RECORD;
    }

    /**
     * Set LCR ID for the change record 
     * 
     * @param id decoded object ID
     */
    public void setId (long id) {
        this.id = id;
    }

    /**
     * Return the LCR ID for the change record
     * 
     * @return change ID, unique to PLOG stream
     */
    public long getId () {
        return this.id;
    }

    /**
     * Set the parent PLOG ID for change record
     * 
     * @param plogId ID of parent PLOG
     */
    public void setPlogId (int plogId) {
        this.plogId = plogId;
    }

    /**
     * Return the ID of parent PLOG
     * 
     * @return ID of parent PLOG for change record
     */
    public int getPlogId () {
        return this.plogId;
    }

    /**
     * Set the Oracle transaction ID of the change
     * 
     * @param transactionId transaction identifier, String
     */
    public void setTransactionId (String transactionId) {
        this.transactionId = transactionId;
    }

    /**
     * Return the transaction ID for the change record
     * 
     * @return identifier of transaction, as string
     */
    public String getTransactionId () {
        return this.transactionId;
    }

    /**
     * Set the ID of LCR of previous save point, this is not used during
     * pessimistic commit
     * 
     * @param savePointId the LCR ID of save point
     */
    public void setSavePointId (long savePointId) {
        this.savePointId = savePointId;
    }

    /**
     * Return the ID of the LCR marked as save point
     * 
     * @return save point LCR ID
     */
    @JsonIgnore
    public long getSavePointId () {
        return this.savePointId;
    }

    /**
     * Set the Oracle SCN number of the change
     * 
     * @param systemChangeNumber Oracle SCN number
     */
    public void setSystemChangeNumber (long systemChangeNumber) {
        this.systemChangeNumber = systemChangeNumber;
    }

    /**
     * Return the Oracle SCN for this change record
     * 
     * @return SCN of change
     */
    public long getSystemChangeNumber () {
        return this.systemChangeNumber;
    }

    /**
     * Set the time stamp for when the change was made in Oracle
     * 
     * @param timestamp change time stamp
     */
    public void setTimestamp (Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Return the time of when change was applied in Oracle
     * 
     * @return change time
     */ 
    public Timestamp getTimestamp () {
        return this.timestamp;
    }

    /**
     * Set the ID of the parent object, table, that this row level change
     * belongs to
     * 
     * @param tableId object ID of parent table
     */
    public void setTableId (int tableId) {
        this.tableId = tableId;
    }

    /**
     * Return the object ID of the parent table of this change
     * 
     * @return ID of parent table
     */
    public int getTableId () {
        return this.tableId;
    }

    /**
     * Set the owner, schema, of parent table of the change values
     * 
     * @param tableOwner parent table owner or schema name for change
     */
    public void setTableOwner (String tableOwner) {
        this.tableOwner = tableOwner;
    }

    /**
     * Return the owner name of the table this change applies to
     * 
     * @return name of parent table owner
     */
    public String getTableOwner () {
        return this.tableOwner;
    }

    /** 
     * Set the name of parent table of the change values
     * 
     * @param tableName parent table name
     */
    public void setTableName (String tableName) {
        this.tableName = tableName;
    }

    /**
     * Return the name of the parent table of the change record
     * 
     * @return name of table that row change values belong to
     */
    public String getTableName () {
        return this.tableName;
    }

    /**
     * Set the actual column values for the row change record. These are a
     * list of column values that are ordered by column ordinal number as
     * encoded in PLOG
     * 
     * @param columnValues List of <em>ColumnValue</em>
     * 
     * @see ColumnValue
     */
    public void setColumnValues (List<ColumnValue> columnValues) {
        this.columnValues = columnValues;
    }

    /**
     * Return the set of column values for the change record, these are a
     * complete set for all columns present merged from OLD/NEW row set.
     * The set contains the column values as present in source after the
     * change action has been applied
     * 
     * @return List of <em>ColumnValue</em>
     */
    public List<ColumnValue> getColumnValues () {
        return this.columnValues;
    }

    /**
     * Set the change action that resulted in this logical change record
     * 
     * @param action the change action
     * 
     * @see ChangeAction
     */
    public void setAction (ChangeAction action) {
        this.action = action;
    }
    
    /**
     * Return the change action that was applied for this logical change
     * record
     * 
     * @return change action for logical change
     */
    public ChangeAction getAction () {
        return this.action;
    }
    
    /**
     * Set whether or not this is a multi-part logical change record, these
     * are transient and merged during parsing, this applies to LOB change
     * records when client sets the domain reader to enable merging of
     * these with it's data counterpart into one logical change record
     * 
     * @param isMultiPart whether or not this is a multi-part record
     */
    public void setIsMultiPart (boolean isMultiPart) {
        this.isMultiPart = isMultiPart;
    }
    
    /**
     * Return whether or not this record is part of a multi-part logical
     * change record, that could possible be merged during parsing, if
     * multi-part merging is enable in domain reader and parsers
     * 
     * @return true if record is multi-part and can be merged, else false
     */
    public boolean isMultiPart () {
        return this.isMultiPart;
    }
    
    /**
     * Check if table owner information was parsed for this change record 
     * 
     * @return true if it has table owner and can be used to link to schema
     *         cache in PLOG, else false if none
     */
    public boolean hasTableOwner () {
        boolean metadata = false;
        
        if (tableOwner != null && tableOwner.length() > 0 && 
            tableName != null && tableName.length() > 0)
        {
            metadata = true;
        }
        return metadata;
    }
    
    /**
     * Check if column data is present for this change record, for No Operation
     * changes, etc. there is no column data, or when internal error has 
     * prevented decoding of column data for a proper change record, these
     * records cannot be persisted
     * 
     * @return true if column values was successfully decoded for this change
     *         record, else false
     */
    public boolean hasColumnData () {
        return columnValues != null && columnValues.size() > 0;
    }
    
    /** 
     * Return data key for this LCR, either supplemental key values or
     * all column values in ordinal number par LOBs
     *  
     * @return String unique key value for this LCR
     */
    @JsonIgnore
    public String getUniqueKey () {
        StringBuilder kb = new StringBuilder();

        for (int c = 0; columnValues != null && c < columnValues.size(); c++)
        {
            ColumnValue cr = columnValues.get (c);
            
            if (cr != null) {
                switch (action) {
                    case INSERT:
                    case LOB_WRITE:
                    case DDL_OPERATION:
                    case NO_OPERATION:
                    case LOB_ERASE:
                    case LOB_TRIM:
                    {
                        if (!cr.getType().equals (ColumnDataType.BLOB)) {
                            if (kb.length() != 0) {
                                kb.append ("_");
                            }
                            
                            /* skip blobs in insert or lob write */
                            kb.append (c + ":" + cr.getValue());
                        }
                        break;
                    }
                    case UPDATE:
                    case DELETE:
                    {
                        /* only supplemental key */
                        if (cr.isSupLogKey()) {
                            if (kb.length() != 0) {
                                kb.append ("_");
                            }
                            
                            kb.append (c + ":" + cr.getValue());
                        }
                        break;
                    }
                    case NONE:
                    default:
                        /* we do nothing */
                        break;
                }
            }
        }
        
        return kb.toString();
    }
    
    /**
     * Return the schema identifier for the logical change record, this is 
     * used for linking to PLOG schema cache, if needed
     * 
     * @return fully qualified schema name as identifier
     */
    @JsonIgnore
    public String getSchemaIdentifier () {
        String schemata = null;
        
        if (hasTableOwner()) {
            schemata = 
                tableOwner.replace("$", "") + "." + 
                tableName.replace("$", "");    
        }
        
        return schemata;
    }

    /**
     * Return summary of LCR contents as text
     * 
     * @return Textual representation of an LCR
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("Change record")
          .append(" XID:")
          .append(
              transactionId != null ? transactionId : "N/A"
          )
          .append(" type:")
          .append(recordType)
          .append(" SCN:")
          .append(
              systemChangeNumber != 0 ? systemChangeNumber : "N/A"
          )
          .append(" owner:")
          .append(
              tableOwner != null ? tableOwner : "N/A"
          )
          .append(" table:")
          .append(
              tableName != null ? tableName : "N/A"
          );
        
        return sb.toString();
    }
    
    /** 
     * Serialize the domain object for LCR to JSON
     * 
     * @return A LCR serialized as a JSON string
     * @throws Exception Failed to serialize LCR to JSON
     */
    @Override
    public String toJSONString () throws Exception {
        return DomainJSONConverter.toJSONString (this);
    }
    
    /**
     * De-serialize JSON logical change record string to domain object
     * 
     * @param json serialized logical change record JSON string
     * 
     * @return logical change record domain object
     * @throws Exception if de-serialization error occur
     */
    public static ChangeRowRecord fromJSONString (String json)
    throws Exception {
        return (ChangeRowRecord)
            DomainJSONConverter.fromJSONString(
                json,
                ChangeRowRecord.class
            );
    }
    
    /**
     * Return the record type for this doman record of type change record
     * 
     * @return domain record type
     */
    @Override
    public DomainRecordType getRecordType() {
        return recordType;
    }

    /**
     * Return the replicate offset of the end of this domain record, needed
     * for filtering, as a composite value made up of PLOG ID and byte
     * offset within that PLOG file
     * 
     * @return the unique replicate offset in the parent PLOG stream, as
     *         a long value
     */
    @Override
    public ReplicateOffset getRecordOffset() {
        return replicateOffset;
    }

    /**
     * Return this domain records parent schema, for change record it's the
     * schema identifier of the row-level change
     * 
     * @return schema name for the change record
     */
    @Override
    @JsonIgnore
    public String getRecordSchema() {
        return getSchemaIdentifier();
    }

    /**
     * Identify all change records with decoded column values as a data
     * record to the domain layer
     * 
     * @return true if this is a valid data record, else false
     */
    @Override
    @JsonIgnore
    public boolean isDataRecord() {
        return columnValues != null && !columnValues.isEmpty();
    }
    
    /**
     * Identify the ID of parent PLOG for this domain record
     * 
     * @return ID of parent PLOG
     */
    @Override
    @JsonIgnore
    public int getParentId () {
        return plogId;
    }

    /**
     * Return the source Oracle system change number for this domain record,
     * eg. the SCN of the actual change
     * 
     * @return SCN value as long
     */
    @Override
    @JsonIgnore
    public Long getRecordSCN() {
        return systemChangeNumber;
    }
    
}
