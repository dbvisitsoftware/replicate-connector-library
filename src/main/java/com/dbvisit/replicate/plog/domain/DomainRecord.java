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

import com.dbvisit.replicate.plog.reader.criteria.FilterableRecord;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Base filterable record for domain reader interface
 */
public abstract class DomainRecord implements FilterableRecord<DomainRecordType>  
{
    /** type of domain record */
    protected DomainRecordType recordType;
    
    /** replicate offset of this domain record */
    protected ReplicateOffset replicateOffset;

    /** whether or not to persist this domain record */
    protected boolean persist = false;
    
    /** whether or not the domain records is complete and ready to be used */
    protected boolean complete = false;
    
    /** encoded size of the raw record it was parsed/converted from */
    protected int rawRecordSize;
    
    /**
     * Set the type of the domain record
     * 
     * @param recordType domain record type
     */
    public void setDomainRecordType (DomainRecordType recordType) {
        this.recordType = recordType;
    }
    
    /**
     * Return the type of this domain record
     * 
     * @return domain record type
     */
    @JsonIgnore
    public DomainRecordType getDomainRecordType () {
        return this.recordType;
    }
    
    /**
     * Set the absolute replicate offset for the end of this domain
     * record in a replication data stream
     * 
     * @param replicateOffset offset of end of record in data stream
     */
    public void setReplicateOffset (ReplicateOffset replicateOffset) {
        this.replicateOffset = replicateOffset;
    }

    /**
     * Return the end stream offset of this replicated domain record
     * 
     * @return absolute offset in the replicate stream
     */
    public ReplicateOffset getReplicateOffset () {
        return this.replicateOffset;
    }

    /**
     * Set whether or not this domain record needs to be persistent to
     * cache, if false it's transient
     * 
     * @param persist true if record needs to persisted to caller or false
     */
    public void setPersist (boolean persist) {
        this.persist = persist;
    }
    
    /**
     * Return whether or not this record should be persisted, eg. passed
     * back to caller of domain reader
     * 
     * @return true if record needs to persisted to caller or false
     */
    @JsonIgnore
    public boolean shouldPersist () {
        return this.persist;
    }

    /**
     * Set whether or not this domain record is deemed complete, this is
     * relevant for multi-part change records and aggregate information
     * records
     * 
     * @param complete true if record is complete, else false
     */
    public void setComplete (boolean complete) {
        this.complete = complete;
    }
    
    /**
     * Return whether or not the domain record is complete and can be used
     * 
     * @return true if complete and ready, else false
     */
    @JsonIgnore
    public boolean isComplete () {
        return complete;
    }

    /**
     * Set the size of raw PLOG entry record(s) in bytes, i.e the record(s)
     * from which this domain record was parsed.
     * 
     * @param rawRecordSize size in bytes of raw PLOG entry record
     */
    public void setRawRecordSize (int rawRecordSize) {
        this.rawRecordSize = rawRecordSize;
    }
    
    /**
     * Return the size, in bytes, of the raw PLOG entry record
     * 
     * @return size of raw record counterpart, in bytes
     */
    @JsonIgnore
    public int getRawRecordSize() {
        return rawRecordSize;
    }
    
    /**
     * Whether or not this domain record is a change record.
     * 
     * @see LogicalChangeRecord
     * 
     * @return true if instance of <em>LogicalChangeRecord</em>, else false
     */
    @JsonIgnore
    public boolean isChangeRecord () {
        return recordType.equals (DomainRecordType.CHANGE_RECORD);
    }
    
    /**
     * Whether or not this domain record is a meta data record.
     * 
     * @see MetaDataRecord
     * 
     * @return true if instance of <em>MetaDataRecord</em>, else false
     */
    @JsonIgnore
    public boolean isMetaDataRecord () {
        return recordType.equals (DomainRecordType.METADATA_RECORD);
    }
    
    /**
     * Whether or not this domain record is a header record.
     * 
     * @see HeaderRecord
     * 
     * @return true if instance of <em>HeaderRecord</em>, else false
     */
    @JsonIgnore
    public boolean isHeaderRecord () {
        return recordType.equals(DomainRecordType.HEADER_RECORD);
    }
    
    /**
     * Whether or not this domain record is an aggregate transaction
     * information record
     * 
     * @see TransactionInfoRecord
     * 
     * @return true if instance of <em>TransactionInfoRecord</em>, else false
     */
    @JsonIgnore
    public boolean isTransactionInfoRecord () {
        return recordType.equals(DomainRecordType.TRANSACTION_INFO_RECORD);
    }
    
    /**
     * Domain record serialized as JSON string, for all sub-classes to
     * implement
     *  
     * @return domain record serialized JSON, as string
     * 
     * @throws Exception when serialization error occurs
     */
    public String toJSONString () throws Exception {
        throw new UnsupportedOperationException(
            "Abstract class cannot be converted to JSON"
        );
    }
    
    /**
     * Domain record's parent PLOG ID
     * 
     * @return parent PLOG ID
     */
    public int getParentId () {
        throw new UnsupportedOperationException(
            "Abstract class cannot have a parent PLOG"
        );
    }

}
