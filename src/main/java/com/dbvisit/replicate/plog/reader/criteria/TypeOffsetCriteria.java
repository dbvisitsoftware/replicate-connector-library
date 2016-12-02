package com.dbvisit.replicate.plog.reader.criteria;

import java.util.Map;

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
 * Filter by record offset for record types
 * 
 * @param <T> Type parameter for filter
 */
public class TypeOffsetCriteria<T> implements Criteria<T> {
    /** Define the replicated offset per record type as criteria to meet */
    private final Map<T, ReplicateOffset> typeOffsets;

    /**
     * Create type offset criteria from the replicated offset each record
     * of defined type must have to meet criteria
     * 
     * @param typeOffsets start replicated offset per record type
     */
    public TypeOffsetCriteria (Map<T, ReplicateOffset> typeOffsets) {
        this.typeOffsets = typeOffsets;
    }

    /**
     * Test if record is present in the replicated stream after the replicate
     * offset defined for its type of record
     * 
     * @param rec filterable record by type and replicate offset
     * 
     * @return true if record meets requirements of offset for its type,
     *         else false
     */
    @Override
    public boolean meetCriteria(FilterableRecord<T> rec) 
    throws Exception {
        if (typeOffsets == null || typeOffsets.isEmpty()) {
            throw new Exception (
                "Invalid un-initialised type offset criteria"
            );
        }

        /* criteria is not met by default */
        boolean pass = false;

        /* apply offset criteria to all records of this type */
        if (typeOffsets.containsKey(rec.getRecordType())) {
            ReplicateOffset typeOffset = typeOffsets.get (rec.getRecordType());

            /* filter all records of type T that is older than the filter's
             * replicate offset */
            if (rec.getRecordOffset().compareTo(typeOffset) > 0) {
                pass = true;
            }
        }

        return pass;
    }

}
