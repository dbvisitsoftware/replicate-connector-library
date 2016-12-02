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

/** 
 * Represent the unique offset for replication record in a stream of PLOGs
 */
public class ReplicateOffset implements Comparable<ReplicateOffset> {
    /** The UID of the PLOG that record is encoded in */
    private long plogUID;
    /** The byte offset within the PLOG that the record is encoded at */
    private long plogOffset;
    
    /** Create empty replicate offset */
    public ReplicateOffset () { }
    
    /** 
     * Create and populate replicate offset from PLOG ID and byte offset
     * 
     * @param plogUID    UID of PLOG
     * @param plogOffset Offset of record within PLOG
     */
    public ReplicateOffset (long plogUID, long plogOffset) {
        this.plogUID    = plogUID;
        this.plogOffset = plogOffset;
    }
    
    /**
     * Set the PLOG UID of record
     * 
     * @param plogUID ID of PLOG of replicated record
     */
    public void setPlogUID (long plogUID) {
        this.plogUID = plogUID;
    }

    /**
     * Get the ID of PLOG for the replicated record
     * 
     * @return ID of PLOG
     */
    public long getPlogUID () {
        return this.plogUID;
    }

    /** 
     * Set the offset of PLOG record within the unique PLOG data stream
     * 
     * @param plogOffset byte offset of replicated record in PLOG
     */
    public void setPlogOffset (long plogOffset) {
        this.plogOffset = plogOffset;
    }
    
    /**
     * Return the byte offset of replicated record from start of it's
     * parent PLOG file
     * 
     * @return byte offset, as long
     */
    public long getPlogOffset () {
        return this.plogOffset;
    }
  
    /**
     * Simple string representation of replicate offset
     * 
     * @return textual representation of object
     */
    public String toString () {
        return 
            "PLOG UID: " + plogUID + " offset:" + plogOffset;
    }
    
    /**
     * Serialize this replicate offset record as JSON string
     * 
     * @return JSON string containing replicate offset data
     * @throws Exception for any serialization errors
     */
    public String toJSONString () throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        
        return mapper.writeValueAsString(this);
    }
    
    /**
     * De-serialize replicate offset record from JSON string
     * 
     * @param json incoming replicate offset record serialized as JSON
     * 
     * @return de-serialized replicate offset record
     * @throws Exception if JSON parse error occurs
     */
    public static ReplicateOffset fromJSONString (String json) 
    throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        
        return mapper.readValue (json, ReplicateOffset.class);
    }

    /**
     * Compare this replicate offset to another by comparing the unique IDs
     * and offset within the PLOG stream
     * 
     * @return 0 if the same offset in replicated stream, -1 if this is 
     *         before the other offset or +1 if this is after the other
     *         offset within the replicated data stream
     */
    @Override
    public int compareTo(ReplicateOffset o) {
        int cmp = 0;
        
        if (plogUID < o.getPlogUID()) {
            cmp = -1;
        }
        else if (plogUID > o.getPlogUID()) {
            cmp = 1;
        }
        else {
            if (plogOffset < o.getPlogOffset()) {
                cmp = -1;
            }
            else if (plogOffset > o.getPlogOffset()) {
                cmp = 1;
            }
        }
        
        return cmp;
    }

}
