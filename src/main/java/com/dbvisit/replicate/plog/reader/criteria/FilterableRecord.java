package com.dbvisit.replicate.plog.reader.criteria;

import com.dbvisit.replicate.plog.domain.ReplicateOffset;

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

/**
 * Interface for all records that needs to be filtered by Criteria
 * 
 * @param <T> Type parameter for filter
 */
public interface FilterableRecord<T> {
    /**
     * Return the type of this record, as defined by the type parameter for
     * filter
     * 
     * @return record type as type parameter &lt;T&gt; for filtering stream
     *         by type
     */
    public T getRecordType ();

    /**
     * Return the replicate offset, as composite of PLOG UID and offset within
     * PLOG, that unique identifies this record within replicate data stream
     * 
     * @return record offset in replicate stream for filtering stream by
     *         offset
     */
    public ReplicateOffset getRecordOffset ();

    /**
     * Return the schema or full owner identifier for this record
     * 
     * @return schema or full owner identifier for filtering stream by
     *         replicated object name
     */
    public String getRecordSchema();

    /**
     * Identify data records, some criteria may choose to ignore all other
     * types of record, what constitutes a data record depends on record
     * type
     * 
     * @return true if this is a data record, else false
     */
    public boolean isDataRecord();

    /**
     * Return the unique system change number for the associated change
     * in the source Oracle system
     * 
     * @return the source change SCN for global filtering of entries in
     *         PLOG stream by SCN
     */
    public Long getRecordSCN();
}
