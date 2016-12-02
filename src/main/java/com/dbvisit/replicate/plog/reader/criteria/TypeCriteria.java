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
 * Filter by record type
 * 
 * @param <T> Type parameter for filter
 */
public class TypeCriteria<T> implements Criteria<T> {
    /** Define the rules for meeting requirements based on record type */
    private final Map<T, Boolean> types;

    /**
     * Create a type criteria from defined set of record types and whether or 
     * not they pass test
     * 
     * @param types pass or fail defined per record type, the type is
     *              parameterized type as defined for filterable record
     */
    public TypeCriteria (Map<T, Boolean> types) {
        this.types = types;
    }

    /**
     * Test if record is of the correct record type to meet criteria
     * 
     * @param rec the filterable record of parameterized type &lt;T&gt; to 
     *            evaluate
     *
     * @return true if the incoming record's type meets the defined criteria,
     *         else false
     */
    @Override
    public boolean meetCriteria(FilterableRecord<T> rec) 
    throws Exception {
        if (types == null || types.isEmpty()) {
            throw new Exception (
                "Invalid un-initialised type criteria"
            );
        }

        /* criteria is not met by default */
        boolean pass = false;

        T type = rec.getRecordType();

        /* lookup the pass criteria for this record type, no entry means it 
         * has to be filtered
         */
        if (types.containsKey (type)) {
            pass = types.get (type);
        }

        return pass;
    }

}
