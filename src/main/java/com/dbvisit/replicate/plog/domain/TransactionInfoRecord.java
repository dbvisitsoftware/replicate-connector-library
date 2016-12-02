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
import java.util.LinkedHashMap;
import java.util.Map;

import com.dbvisit.replicate.plog.domain.util.DomainJSONConverter;
import com.dbvisit.replicate.plog.file.PlogFile;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Transaction information record that represents aggregation of certain
 * properties of all changes in transaction 
 */
public class TransactionInfoRecord extends DomainRecord {
    /** Oracle transaction ID */
    private String id;
    /** ID of PLOG in which transaction started */
    private int startPlogId;
    /** ID of PLOG in which transaction ended */
    private int endPlogId;
    /** SCN of start of data transaction */
    private long startSCN;
    /** SCN of end of data transaction */
    private long endSCN;
    /** Time when data transaction started */
    private Timestamp startTime;
    /** Time when data transaction ended, was committed */
    private Timestamp endTime;
    /** ID of first change record in this data transaction */
    private long startRecordId;
    /** ID of end change record in this data transaction */
    private long endRecordId;
    /** Count of change records in this data transaction */
    private int recordCount;
    /** Count of change records per schema in transaction */
    final private Map<String, Integer> schemaRecordCounts;
    
    /** Reference to the plog this record is cached in, internal only */
    @JsonIgnore
    private PlogFile plog;
    
    /**
     * Create empty transaction information aggregate record of the correct
     * domain type
     */
    public TransactionInfoRecord () {
        recordType = DomainRecordType.TRANSACTION_INFO_RECORD;
        schemaRecordCounts = new LinkedHashMap<String, Integer>();
    }

    /**
     * Set the transaction ID for the aggregrate record
     * 
     * @param id Transaction identifier
     */
    public void setId (String id) {
        this.id = id;
    }

    /**
     * Return the transaction identifier for the aggregate record
     * 
     * @return Transaction identifier
     */
    public String getId () {
        return this.id;
    }

    /**
     * Set the ID of the PLOG of the first change record in this
     * transaction
     * 
     * @param startPlogId PLOG ID of first transaction record
     */
    public void setStartPlogId (int startPlogId) {
        this.startPlogId = startPlogId;
    }

    /**
     * Get the ID of the PLOG of the first record in transaction
     * 
     * @return PLOG ID of first record
     */
    public int getStartPlogId () {
        return this.startPlogId;
    }

    /**
     * Set the ID of the PLOG of the last record in transaction
     * 
     * @param endPlogId PLOG ID of last transaction record
     */
    public void setEndPlogId (int endPlogId) {
        this.endPlogId = endPlogId;
    }

    /**
     * Get the PLOG ID of last record in transaction
     * 
     * @return PLOG ID of last record 
     */
    public int getEndPlogId () {
        return this.endPlogId;
    }

    /**
     * Set the SCN of the first record in transaction
     * 
     * @param startSCN SCN of first record in transaction in PLOG
     */
    public void setStartSCN (long startSCN) {
        this.startSCN = startSCN;
    }

    /**
     * Return the SCN of the first record in transaction, not necessarily
     * a data change record, however all data records will start after this
     * SCN
     * 
     * @return start SCN of transaction
     */
    public long getStartSCN () {
        return this.startSCN;
    }

    /**
     * Set the SCN of the last record in transaction
     * 
     * @param endSCN SCN of last record in transaction in PLOG
     */
    public void setEndSCN (long endSCN) {
        this.endSCN = endSCN;
    }

    /**
     * Return the SCN of the last record in transaction, not necessarily
     * a data change record, however all data records will precede this
     * SCN
     * 
     * @return end SCN of transaction
     */
    public long getEndSCN () {
        return this.endSCN;
    }

    /**
     * Set start time of transaction, as in, time when first change was
     * made in transaction, not necessarily associated with a data change 
     * 
     * @param startTime time when transactional changes started
     */
    public void setStartTime (Timestamp startTime) {
        this.startTime = startTime;
    }

    /**
     * Return time when changes in transaction was started
     * 
     * @return start time of changes in transaction
     */
    public Timestamp getStartTime () {
        return this.startTime;
    }

    /**
     * Set end time of transaction, the time when last change was made
     * 
     * @param endTime time when changes ended
     */
    public void setEndTime (Timestamp endTime) {
        this.endTime = endTime;
    }

    /**
     * Return the end time of changes in the transaction
     * 
     * @return end time of transactional changes
     */
    public Timestamp getEndTime () {
        return this.endTime;
    }

    /**
     * Set the ID of the first record in the transaction, these include
     * all records, not just data change records
     * 
     * @param startRecordId LCR ID of first record
     */
    public void setStartRecordId (long startRecordId) {
        this.startRecordId = startRecordId;
    }

    /**
     * Return the ID of the first record in the transaction
     * 
     * @return start LCR ID of transaction
     */
    public long getStartRecordId () {
        return this.startRecordId;
    }

    /**
     * Set the ID of the last record in the transaction, not necessarily
     * a data change record
     * 
     * @param endRecordId LCR ID of last record in PLOG transaction
     */
    public void setEndRecordId (long endRecordId) {
        this.endRecordId = endRecordId;
    }

    /**
     * Return the ID of the last change record in the transaction in 
     * replicated PLOG stream
     * 
     * @return end LCR ID of transaction
     */
    public long getEndRecordId () {
        return this.endRecordId;
    }

    /**
     * Set the total data record count for the aggregate transaction, these
     * are the number of data records decoded
     * 
     * @param recordCount count of records in transaction
     */
    public void setRecordCount (int recordCount) {
        this.recordCount = recordCount;
    }

    /**
     * Return total number of change records in transaction
     * 
     * @return total record count in transaction
     */
    public int getRecordCount () {
        return this.recordCount;
    }
    
    /**
     * Return counts of data change records for each schema involved in
     * transaction
     *
     * @return map of schemas in transaction with their respective row counts
     */
    public Map<String, Integer> getSchemaRecordCounts () {
        return this.schemaRecordCounts;
    }
    
    /** 
     * Set parent PLOG file of the transaction cache
     * 
     * @param plog PLOG file to hold cached transaction aggregates
     */
    public void setPlog (PlogFile plog) {
        this.plog = plog;
    }
    
    /**
     * Return the PLOG which contains this record in it's cache, usually
     * the start PLOG of transaction
     * 
     * @return the parent PLOG file of the transaction cache
     */
    @JsonIgnore
    public PlogFile getPlog () {
        return this.plog;
    }
    
    /**
     * Return the domain record type as transaction information
     * 
     * @return domain type for transaction
     */
    @Override
    public DomainRecordType getRecordType() {
        return recordType;
    }

    /**
     * Return unique replicate offset of the last change record in this
     * transaction
     * 
     * @return replicate offset of the last change record
     */
    @Override
    public ReplicateOffset getRecordOffset() {
        /* for transaction is must be offset of end of last LCR */
        return replicateOffset;
    }
    
    /**
     * A transaction may involve multiple replicated schemas, not just one,
     * instead return N/A
     * 
     * @return N/A no record schema
     */
    @Override
    @JsonIgnore
    public String getRecordSchema() {
        return "N/A";
    }

    /**
     * This is not a data record, but an aggregate
     * 
     * @return false, this is not a data record
     */
    @Override
    @JsonIgnore
    public boolean isDataRecord() {
        return false;
    }
    
    /**
     * Transaction record may possible span PLOG sequences in stream
     * and has to maintain a cache of transaction info records, this
     * function allows removing it when it's complete and we're done
     */
    public void removeFromPlogCache() {
        if (plog != null) {
            plog.removeTransactionRecordFromCache(this);
        }
        plog = null;
    }
    
    /**
     * Increment total data LCR count for this transaction
     */
    public void incrementRecordCount () {
        recordCount++;
    }
    
    /**
     * Increment the data LCR count for a specific replicate schema
     * involved in this transaction
     *  
     * @param schema Schema identifier for replicated table involved in
     *               this transaction
     */
    public void incrementSchemaRecordCount (String schema) {
        Integer count = schemaRecordCounts.get (schema);
        
        if (count == null) {
            count = 0;
        }
        
        if (schema == null) {
            schema = "N/A";
        }
        schemaRecordCounts.put (schema, ++count);
    }
    
    /**
     * Determine if this record is valid and can be used
     * 
     * @return true if all properties are correctly set and complete, else
     *         false
     */
    @JsonIgnore
    public boolean isValid () {
        return id         != null &&
               startTime  != null &&
               endTime    != null &&
               startSCN   != 0L &&
               endSCN     != 0L &&
               startRecordId != 0L &&
               endRecordId   != 0L &&
               recordCount   > 0;
    }
    
    /** 
     * Serialize the domain object for transaction information to JSON.
     * 
     * @return A transaction info data record serialized as a JSON string
     * @throws Exception Failed to serialize Transaction data record to JSON
     */
    @Override
    public String toJSONString () throws Exception {
        return DomainJSONConverter.toJSONString (this);
    }
    
    /**
     * De-serialize JSON transaction information record string to domain object
     * 
     * @param json serialized transaction information record string
     * 
     * @return transaction info domain record
     * @throws Exception if de-serialization error occur
     */
    public static TransactionInfoRecord fromJSONString (String json) 
    throws Exception 
    {
        return (TransactionInfoRecord)
            DomainJSONConverter.fromJSONString(
                json,
                TransactionInfoRecord.class
            );
    }
    
    /**
     * Return the parent PLOG ID
     * 
     * @return the ID of parent PLOG of this transaction
     */
    @Override
    @JsonIgnore
    public int getParentId () {
        return startPlogId;
    }

    /**
     * Return the start SCN for transaction as the record's SCN
     * 
     * @return start SCN of transaction
     */
    @Override
    public Long getRecordSCN() {
        return startSCN;
    }
    
    /**
     * Return a textual representation of info record
     * 
     * @return object as string
     */
    public String toString() {
        return 
            "TX info record - " + 
             "xid: "        + id            + " "  +
             "start time: " + startTime     + " " +
             "end time: "   + endTime       + " " +
             "start SCN: "  + startSCN      + " " +
             "end SCN: "    + endSCN        + " " +
             "start LCR ID" + startRecordId + " " +
             "end LCR ID "  + endRecordId   + " " +
             "LCR count:"   + recordCount;
    }

}
