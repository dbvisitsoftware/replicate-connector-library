package com.dbvisit.replicate.plog.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A control header record encoded in PLOG that holds the information
 * required during decoding of PLOG entries and the feature set
 * present in PLOG
 */
public class HeaderRecord extends DomainRecord {
    /** The raw object ID in PLOG */
    private long id;
    /** The parent PLOG ID for this header record */ 
    private int plogId;
    /** All domain records must be filterable by system change number */
    private long scn;
    /** UUID for MINE process that created this PLOG file */
    private String mineUUID;
    /** 
     * Whether or not the entries in PLOG has their dictionary information
     * encoded compactly, eg. only present in first entry for table in PLOG
     */
    private boolean compactEncoding;
    /** Whether or not the dictionary records ithe PLOG are encoded as JSON */
    private boolean jsonDictionary;
    /** Whether or not all transactions in PLOG are committed, no rollbacks */
    private boolean pessimisticCommit;
    /** 
     * Whether or not all transaction in PLOG have been serialized, eg.
     * can be replayed as is, already in correct order
     */
    private boolean serializedTransactions;
    
    /**
     * Create an empty PLOG header record of the correct domain record type
     */
    public HeaderRecord () {
        recordType = DomainRecordType.HEADER_RECORD;
    }
    
    /**
     * Set the object ID for header record
     * 
     * @param id object ID decoded from PLOG entry record
     */
    public void setId (long id) {
        this.id = id;
    }

    /**
     * Return the raw object ID of this header record
     * 
     * @return PLOG object ID, as integer
     */
    public long getId () {
        return this.id;
    }

    /** 
     * Set the ID of the parent PLOG for which these properties are valid
     * 
     * @param plogId the ID of PLOG decoded from PLG entry record
     */
    public void setPlogId (int plogId) {
        this.plogId = plogId;
    }

    /**
     * Return the ID of parent PLOG
     * 
     * @return PLOG ID of this header, as integer
     */
    public int getPlogId () {
        return this.plogId;
    }
    
    /**
     * Set the system change number of the header, never used, but to 
     * conform to DomainRecord
     * 
     * @param scn Oracle SCN value for record, as long
     */
    public void setSCN (long scn) {
        this.scn = scn;
    }
    
    /**
     * Return the SCN for this record, never used
     * 
     * @return SCN for record
     */
    public long getSCN () {
        return this.scn;
    }
    
    /**
     * Set the UUID of the MINE process that created this PLOG file 
     * 
     * @param mineUUID UUID of MINE process
     */
    public void setMineUUID (String mineUUID) {
        this.mineUUID = mineUUID;
    }
    
    /**
     * Return the UUID of the MINE process that created this PLOG file
     * 
     * @return UUID of MINE process
     */
    public String getMineUUID () {
        return this.mineUUID;
    }
    
    /**
     * Set whether or not the row-level dictionary information is encoded
     * in a compact form
     * 
     * @param compactEncoding true if first row record in PLOG for each table
     *                        has dictionary properties, rest not, else false
     *                        if all records have dictionary
     */
    public void setCompactEncoding (boolean compactEncoding) {
        this.compactEncoding = compactEncoding;
    }
    
    /**
     * Return whether or not the row-level dictionary information is encoded
     * in a compact form
     * 
     * @return true if PLOG has compactly encoded dictionary, else false
     */
    @JsonProperty ("compactEncoding")
    public boolean hasCompactEncoding () {
        return this.compactEncoding;
    }

    /**
     * Set whether or not the parent PLOG of header record has schema meta
     * data dictionary encoded as JSON document
     * 
     * @param jsonDictionary true if JSON is present, else false
     */
    public void setJsonDictionary (boolean jsonDictionary) {
        this.jsonDictionary = jsonDictionary;
    }

    /** 
     * Check whether or not the parent PLOG has dictionary information
     * encoded as a JSON document
     *  
     * @return true if JSON needs to be decoded, else false
     */
    @JsonProperty ("jsonDictionary")
    public boolean hasJsonDictionary () {
        return this.jsonDictionary;
    }

    /**
     * Set whether or not the parent PLOG's transactions are all committed,
     * as in no rollbacks
     * 
     * @param pessimisticCommit true if all transactions have completed, eg.
     *                          pessimisticly committed, else false
     */
    public void setPessimisticCommit (boolean pessimisticCommit) {
        this.pessimisticCommit = pessimisticCommit;
    }

    /**
     * Check whether or not the parent PLOG has all transactions committed
     * 
     * @return true, if all transactions are pessimisticly committed, 
     *         else false
     */
    @JsonProperty ("pessimisticCommit")
    public boolean hasPessimisticCommit () {
        return this.pessimisticCommit;
    }
    
    /**
     * Set whether or not all transactions in parent PLOG has been serialized,
     * can be replayed in encoded order
     * 
     * @param serializedTransactions true if all transactions encoded in 
     *                               commit order, else false
     */
    public void setSerializedTransactions (boolean serializedTransactions) {
        this.serializedTransactions = serializedTransactions;
    }
    
    /**
     * Check whether or not all transactions in parent PLOG have already
     * been serialized
     * 
     * @return true if all transactions in encoded commit order, else false 
     */
    @JsonProperty ("serializedTransactions")
    public boolean hasSerializedTransactions () {
        return this.serializedTransactions;
    }

    /**
     * Return this record type as type PLOG control header
     * 
     * @return header record type
     */
    @Override
    public DomainRecordType getRecordType() {
        return recordType;
    }

    /**
     * Return the header record's replicate offset, need to conform to
     * <em>FilterableRecord</em> for <em>Criteria</em>
     * 
     * @return unique offset for end of this record within a replicate stream
     */
    @Override
    public ReplicateOffset getRecordOffset() {
        return replicateOffset;
    }

    /**
     * Return the name of parent schema for this record, in this case N/A
     * 
     * @return N/A
     */
    @Override
    @JsonIgnore
    public String getRecordSchema() {
        return "N/A";
    }

    /**
     * This is not a data record, needed by <em>FilterableRecord</em>
     * 
     * @return always false, this is not a data record
     */
    @Override
    @JsonIgnore
    public boolean isDataRecord() {
        return false;
    }
    
    /**
     * Return whether or not the parent PLOG is considered valid for
     * processing by downstream clients that expect JSON dictionary
     * and committed data only
     * 
     * @return true if valid, else false
     */
    @JsonIgnore
    public boolean isValid () {
        return jsonDictionary && pessimisticCommit;
    }

    /** 
     * Serialize the domain object for header data
     * 
     * @return A header data record serialized as a JSON string
     * 
     * @throws Exception Unable to serialize Header record to JSON
     */
    @Override
    public String toJSONString () throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        
        return mapper.writeValueAsString(this);
    }
    
    /**
     * Return the parent PLOG ID
     * 
     * @return parent PLOG ID
     */
    @Override
    @JsonIgnore
    public int getParentId () {
        return plogId;
    }

    /**
     * Return the SCN of this header record, can never be applied as filter
     * because it's compulsory to read headers always, however it is needed
     * by <em>FilterableRecord</em>
     * 
     * @return SCN for record
     */
    @Override
    public Long getRecordSCN() {
        return scn;
    }
    
}
