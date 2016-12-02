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

/**
 * Schema (object owner) criteria for data records
 * 
 * @param <T> Type parameter for filter
 */
public class SchemaCriteria<T> implements Criteria<T> {
    /** Define the meet criteria for schemas */
    private final Map<String, Boolean> schemas;

    /**
     * Create a schema criteria from a defined white/black list of
     * schemas, as in true means schema that meets will pass, black is false
     * which means schema will always fail and will be omitted
     * 
     * @param schemas the meet criteria for schemas by schema (owner) name
     */
    public SchemaCriteria (Map<String, Boolean> schemas) {
        this.schemas = schemas;
    }

    /**
     * Test to see if the schema owner or parent of filterable record is 
     * defined in white list to pass or black list to fail. If not present
     * in criteria lookup and the record is a data record it will by
     * definition not meet the requirements and fail, if it's not a data
     * record it cannot be evaluated without a parent is passes.
     * 
     * @param rec a record to test if it's a child of a schema that meets
     *            the criteria or not
     *
     * @return true if this record's schema is defined in white list or
     *         has no parent and is not a data record, else false if it
     *         is in black list or not present at all
     */
    @Override
    public boolean meetCriteria(FilterableRecord<T> rec)
    throws Exception {
        if (schemas == null || schemas.isEmpty()) {
            throw new Exception (
                "Invalid un-initialised schema name criteria"
            );
        }

        String schemaName = rec.getRecordSchema();

        boolean pass = false;

        /* no schema name means that it has been filtered in meta data
         * dictionary already
         */
        if (rec.isDataRecord() && schemaName != null) {
            if (schemas.containsKey (schemaName)) {
                pass = schemas.get (schemaName);
            }
            else if (schemas.containsKey (schemaName.toLowerCase())) {
                pass = schemas.get (schemaName.toLowerCase());
            }
            else if (schemas.containsKey (schemaName.toUpperCase())) {
                pass = schemas.get (schemaName.toUpperCase());
            }
        }
        else if (!rec.isDataRecord()) {
            /* not actual data records, let it through */
            pass = true;
        }

        return pass;
    }

}
