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

import java.util.LinkedHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Global criteria for filtering all records, irrespective of type,
 * by the SCN for the original change in Oracle
 * 
 * @param <T> type parameter
 */
public class SystemChangeNumberCriteria<T> implements Criteria<T> {
    /** Default no schema to not available */
    private static final String NO_SCHEMA = "N/A";
    /** 
     * SCN offsets per schema, supporting wildcards, sorted
     * by priority
     */
    private final LinkedHashMap<String, Long> scnOffsets;

    /**
     * Creates a global SCN criteria from a single system change value
     * to apply as filter for all records by owner, irrespective of
     * type or owner name
     * 
     * @param scn only records with SCNs higher than this value will meet
     *            the SCN criteria
     */
    @SuppressWarnings("serial")
    public SystemChangeNumberCriteria (final long scn) {
        /* global SCN filter */
        scnOffsets = new LinkedHashMap<String, Long>() {{
            put ("*", scn);
        }};
    }

    /**
     * Creates a SCN criteria from a SCN offset defined for each record
     * schema owner to process
     * 
     * @param scnOffsets SCN offsets per record schema
     */
    public SystemChangeNumberCriteria(
        final LinkedHashMap<String, Long> scnOffsets
    ) {
        this.scnOffsets = scnOffsets;
    }

    /**
     * Test to see if the SCN value of the incoming filterable record
     * meets the defined global or schema based SCN offset criteria
     * 
     * @param rec filterable record that has an SCN to test
     * 
     * @return true if it meets the SCN criteria, else false
     */
    @Override
    public boolean meetCriteria(FilterableRecord<T> rec)
    throws Exception {
        if (rec == null) {
            throw new Exception (
              "Invalid record provided for internal schema filter criteria, " + 
              "reason: empty record" 
            );
        }

        if (scnOffsets == null || scnOffsets.isEmpty()) {
            throw new Exception (
                "Invalid un-initialised SCN criteria for schemas"
            );
        }

        /* fail, unless a record passes the defined SCN criteria */
        boolean pass = false;

        /* we default to N/A string, this is done so that global wildcard
         * SCN filter can be applied to all records
         */
        String schemaName = rec.getRecordSchema() != null 
                ? rec.getRecordSchema()
                : NO_SCHEMA;

        /* by priority */
        for (String f : scnOffsets.keySet()) {
            /* get start SCN value for regexp */
            long startSCN = scnOffsets.get (f);

            /* has an SCN offset greater or equal to zero */
            if (startSCN >= 0) {
                Pattern regex = Pattern.compile(
                    CriteriaUtility.wildcardToRegex (f)
                );

                Matcher m = regex.matcher(schemaName);

                /* apply SCN criteria for records in a schema that matches
                 * regular expression */
                if (m.matches()) {
                    /* first failure or pass is returned, rest is ignored */
                    if (rec.getRecordSCN() >= startSCN) {
                        pass = true;
                    }
                    else {
                        pass = false;
                    }
                    break;
                }
            }
        }

        return pass;
    }

}
