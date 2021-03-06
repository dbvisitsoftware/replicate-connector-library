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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.databind.DeserializationFeature;

import com.dbvisit.replicate.plog.domain.util.JSONConverterView;

/**
 * Provide the details of a replicated schema in the replicate data stream 
 */
public class ReplicateInfo {
    /** The unique ID of PLOG that first contained this replicated schema */
    private Long plogUID;
    /** The PLOG offset for start of data */
    private Long dataOffset;
    /** The full identifier for replicated object */
    private String identifier;
    /** Replicated schema has been sent, internal and not public by default */
    private boolean sent = false;
    /** Aggregate use only, may not be real replicated object */
    private boolean aggregate = false;
    
    private static final ObjectMapper mapper = 
        new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
            .configure(
                DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, 
                false
            );
    
    /**
     * Set the UID of PLOG where the replicate schema is defined at
     * 
     * @param plogUID unique ID of PLOG within replication
     */
    public void setPlogUID (Long plogUID) {
        this.plogUID = plogUID;
    }

    /**
     * Return the UID of PLOG containing the definition of the replicated schema
     * 
     * @return unique ID of PLOG, a composite of its ID and time stamp
     */
    public Long getPlogUID () {
        return this.plogUID;
    }

    /**
     * Set the byte offset in parent PLOG where the meta data definition
     * record for this replicated schema is encoded, that is the start
     * of the record, the byte offset to which to reader should be 
     * forwarded to parse this replicated schema
     * 
     * @param dataOffset the byte offset of JSON meta data record for
     *        this schema in PLOG
     */
    public void setDataOffset (Long dataOffset) {
        this.dataOffset = dataOffset;
    }

    /**
     * Return the byte offset of JSON meta data record for a replicated
     * schema in PLOG
     * 
     * @return byte offset in parent PLOG of start of replicated table
     */
    public Long getDataOffset () {
        return this.dataOffset;
    }

    /**
     * The full schema identifier for the replicated table
     * 
     * @param identifier replicated schema (fully qualified table) name
     */
    public void setIdentifier (String identifier) {
        this.identifier = identifier;
    }

    /**
     * Return the string identifier of the replicated table in data stream
     * 
     * @return name of replicated schema
     */
    public String getIdentifier () {
        return this.identifier;
    }

    /**
     * Set whether or not this replication information have been sent to 
     * the caller or client, this property is used to identify when to
     * maintain replicate info and when to start acting on it, at caller
     * 
     * @param sent true if if has been sent and processed by caller, else
     *             false
     */
    public void setSent (boolean sent) {
        this.sent = sent;
    }
    
    /**
     * Check whether or not this replicate info record has been sent
     * and processed at the callers' end. This is only used by internal
     * view and not used by public view
     * 
     * @return true if it has been processed, else false
     */
    @JsonProperty("sent") @JsonView(JSONConverterView.Internal.class)
    public boolean sent () {
        return this.sent;
    }
    
    /**
     * Set whether or not the schema associated with replicate is real or
     * an aggregate, which may not be real, like the transaction information
     * records stream
     * 
     * @param aggregate true if replicated object is an aggregate and not a 
     *                  real table, else false
     */
    public void setAggregate (boolean aggregate) {
        this.aggregate = aggregate;
    }
    
    /** 
     * Return whether or not the replicated object is real or an aggregate
     *  
     * @return true if an aggregate, else false for real table
     */
    public boolean isAggregate () {
        return this.aggregate;
    }
    
    /**
     * Return hash code on schema identifier converted to lower case; 
     * ignore which PLOG it belongs to
     * 
     * @return hash code for object
     */
    @Override
    public int hashCode () {
        int h = 0;
        if (identifier != null) {
            /* only on schema name */
            h = identifier.toLowerCase().hashCode();
        }
        else {
            h = super.hashCode();
        }
        
        return h;
    }

    /**
     * Return string representation of this replicate information object
     * 
     * @return String 
     */
    public String toString () {
        StringBuilder sb = new StringBuilder();
        
        sb.append("Replicate info")
          .append(" UID: ")
          .append(plogUID != null ? plogUID : -1L)
          .append(" offset: ")
          .append(dataOffset != null ? dataOffset : -1L)
          .append(" identifier: ")
          .append(identifier != null ? identifier : "n/a")
          .append(" sent: ")
          .append(sent)
          .append(" aggregate: ")
          .append(aggregate);
        
        return sb.toString();
    }

    /**
     * Serialize this replicate info record as JSON string
     * 
     * @return JSON string 
     * @throws Exception for any serialization errors
     */
    public String toJSONString () throws Exception {
        return mapper.writeValueAsString(this);
    }
    
    /**
     * Serialize this replicate info record either using public or
     * internal JSON format
     * 
     * @param publicView use public format for JSON, else internal
     * 
     * @return JSON string 
     * @throws Exception for any serialization errors
     */
    public String toJSONString (boolean publicView) throws Exception {
        String json = null;
        
        if (publicView) {
            json = mapper.writerWithView(
                JSONConverterView.Public.class
            ).writeValueAsString(this);
        }
        else {
            json = mapper.writeValueAsString(this);
        }
        return json;
    }
    
    /**
     * De-serialize replicate information record from JSON string
     * 
     * @param json incoming replicate information serialized as JSON
     * 
     * @return de-serialized replicate info record
     * @throws Exception if JSON parse error occurs
     */
    public static ReplicateInfo fromJSONString (String json) 
    throws Exception {
        return mapper.readValue (json, ReplicateInfo.class);
    }
    
}
