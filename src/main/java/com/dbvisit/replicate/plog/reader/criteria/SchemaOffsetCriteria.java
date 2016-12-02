package com.dbvisit.replicate.plog.reader.criteria;

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

import java.util.Map;

import com.dbvisit.replicate.plog.domain.ReplicateOffset;

/**
 * Schema skip offset criteria for data LCRs
 * 
 * @param <T> Type parameter for filter
 */
public class SchemaOffsetCriteria<T> implements Criteria<T> {
    /** Data skip offset for schema identifiers */
    private final Map <String, ReplicateOffset> schemaOffsets;

    /** 
     * Create schema offset criteria from the data skip offsets defined per
     * record owner
     *
     * @param schemaOffsets offset of data records for each schema, if missing
     *                      it fails by default
     */
    public SchemaOffsetCriteria (Map <String, ReplicateOffset> schemaOffsets){
        this.schemaOffsets = schemaOffsets;
    }

    /**
     * Test to see if offset of a data record within replication meets the
     * requirements for defined in criteria for it's schema owner, by
     * default if it's schema owner is not present it means it must
     * be filtered, this is a white list criteria
     * 
     * @param rec filterable record to test if it must be processed based
     *            on its offset in the replicated stream for its parent
     *            schema
     * 
     * @return true if a data record meets the offset criteria for its parent
     *         schema or is not a data record, else false if it before the 
     *         schema offset in the replication or not present in white list
     */
    @Override
    public boolean meetCriteria(FilterableRecord<T> rec)
    throws Exception {
        if (schemaOffsets == null || schemaOffsets.isEmpty()) {
            throw new Exception (
                "Invalid un-initialised skip offset criteria"
            );
        }

        /* criteria is not met by default */
        boolean pass = false;

        /* criteria only applies for data records */
        if (rec.isDataRecord()) {
            String schema = rec.getRecordSchema();
            ReplicateOffset schemaOffset = null;

            if (schema == null && rec.getRecordOffset() == null) {
                throw new Exception (
                    "Invalid data record provided for skip offset criteria, " +
                    "reason: missing schema identifier and unique offset"
                );
            }

            if (schemaOffsets.containsKey (schema)) {
                schemaOffset = schemaOffsets.get (schema);
            }
            else if (schemaOffsets.containsKey (schema.toLowerCase())) {
                schemaOffset = schemaOffsets.get (schema.toLowerCase());
            }
            else if (schemaOffsets.containsKey (schema.toUpperCase())) {
                schemaOffset = schemaOffsets.get (schema.toUpperCase());
            }

            if (schemaOffset != null) {
                /* skip offset is the offset until which to skip parsing */
                if (rec.getRecordOffset().compareTo(schemaOffset) > 0) {
                    pass = true;
                }
            }
            else {
                /* no schema offset criteria for record's parent schema */
                pass = true;
            }
        }
        else {
            /* do not filter non data records by offset */
            pass = true;
        }

        return pass;
    }

}
