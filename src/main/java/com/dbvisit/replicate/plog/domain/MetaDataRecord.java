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

import com.dbvisit.replicate.plog.domain.util.DomainJSONConverter;
import com.dbvisit.replicate.plog.metadata.DDLMetaData;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * The meta data that defines a replicated schema as originally created
 * or modified in source Oracle
 */
public class MetaDataRecord extends DomainRecord {
    /** The ID of the LCR which had the JSON meta data as payload */
    private long id;
    /** The ID of the PLOG from which this record was parsed */
    private int plogId;
    /** The SCN of the LCR which had the JSON meta data as payload */
    private long scn;
    /** The meta data for the data definition change */
    private DDLMetaData metaData;
    
    /** Create an empty meta data record with correct domain record type */
    public MetaDataRecord () {
        recordType = DomainRecordType.METADATA_RECORD;
    }

    /**
     * Set the ID of change record this meta data record was decoded from
     * 
     * @param id LCR ID
     */
    public void setId (long id) {
        this.id = id;
    }

    /**
     * Get the ID of the change record for this meta data record, act as
     * unique ID for the meta data record proper
     * 
     * @return ID of meta data LCR
     */
    public long getId () {
        return this.id;
    }

    /**
     * Set the ID of parent PLOG from which this meta data record was parsed
     * 
     * @param plogId ID of PLOG in stream
     */
    public void setPlogId (int plogId) {
        this.plogId = plogId;
    }

    /**
     * Get the ID of parent PLOG
     * 
     * @return PLOG ID
     */
    public int getPlogId () {
        return this.plogId;
    }

    /**
     * Set the meta data parsed from JSON payload of LCR
     * 
     * @param metaData meta data for data change definition
     */
    public void setMetaData (DDLMetaData metaData) {
        this.metaData = metaData;
    }

    /**
     * Return the meta data definition for the data 
     * @return meta data definition
     * 
     * @see com.dbvisit.replicate.plog.metadata.DDLMetaData
     */
    public DDLMetaData getMetaData () {
        return this.metaData;
    }
    
    /** 
     * Set the Oracle system change number for change record that had
     * JSON dictionary as payload
     * 
     * @param scn Oracle SCN value for LCR
     */
    public void setSCN (long scn) {
        this.scn = scn;
    }
    
    /**
     * Get the SCN for the data change definition record
     * 
     * @return Oracle SCN value
     */
    public long getSCN () {
        return this.scn;
    }
    
    /**
     * Return the type of domain record as meta data
     * 
     * @return type as meta data domain record
     */
    @Override
    public DomainRecordType getRecordType() {
        return recordType;
    }

    /**
     * Return the replicate offset of the end of this meta data record
     * 
     * @return replicate offset of record
     */
    @Override
    public ReplicateOffset getRecordOffset() {
        return replicateOffset;
    }

    /**
     * Return the name of the schema defined by this meta data record
     * 
     * @return name of schema of meta data
     */
    @Override
    @JsonIgnore
    public String getRecordSchema() {
        return metaData.getSchemataName();
    }
    
    /** 
     * Serialize the domain object for meta data to JSON.
     * 
     * @return A meta data record serialized as a JSON string
     * @throws Exception Unable to serialize Meta data record to JSON
     */
    @Override
    public String toJSONString () throws Exception {
        return DomainJSONConverter.toJSONString (this);
    }
    
    /**
     * De-serialize JSON meta data record string to domain object
     * 
     * @param json serialized meta data record string
     * 
     * @return meta data domain record
     * @throws Exception if de-serialization error occur
     */
    public static MetaDataRecord fromJSONString (String json) 
    throws Exception {
        return (MetaDataRecord)
            DomainJSONConverter.fromJSONString(
                json,
                MetaDataRecord.class
            );
    }

    /**
     * This is not a data record, always return false
     * 
     * @return false, this is not a data record
     */
    @Override
    @JsonIgnore
    public boolean isDataRecord() {
        return false;
    }

    /**
     * Return the ID of the PLOG that this meta data record was decoded
     * from
     * 
     * @return ID of parent PLOG
     */
    @Override
    @JsonIgnore
    public int getParentId () {
        return plogId;
    }

    /**
     * Return the SCN of the LCR for this meta data record
     * 
     * @return SCN of meta data LCR
     */
    @Override
    public Long getRecordSCN() {
        return scn;
    }
    
}
