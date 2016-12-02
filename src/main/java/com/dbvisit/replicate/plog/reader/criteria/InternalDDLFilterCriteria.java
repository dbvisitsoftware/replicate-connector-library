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

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Filter all DDL for internal tables by schema name, these should not
 * to be processed. Simple wildcards are supported, eg. SYS.* will filter
 * all tables owned by SYS.
 *  
 * @param <T> Type parameter for filter
 */
public class InternalDDLFilterCriteria<T> implements Criteria<T> {
    /** Static lookup of internal schemas to filter */
    @SuppressWarnings("serial")
    private final Map<String, Boolean> filter =
        new HashMap<String, Boolean>() {{
            put ("SYS.*", true);
            put ("DBVREP.*", true);
            put ("DBVREP*.*", true);
    }};

    /**
     * Test to see if the incoming record, abiding by the behavior of a
     * <em>FilterableRecord</em>, is not a child of internal Oracle or
     * replicate dictionary tables, the replicate objects for internal
     * objects will always be filtered
     * 
     * @param rec record to test against internal DDL filter criteria
     * 
     * @return true if this record is not part of internal objects, else
     *         false if it is
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

        String schemaName = rec.getRecordSchema();

        /* pass by default, this is a filter */
        boolean pass = true;

        if (schemaName != null) {
            for (String f : filter.keySet()) {
                if (filter.get (f) == true) {
                    Pattern regex = Pattern.compile(
                        CriteriaUtility.wildcardToRegex (f)
                    );

                    Matcher m = regex.matcher(schemaName);
                    /* filter record if it's schema matches the regexp */
                    if (m.matches()) {
                        pass = false;
                        break;
                    }
                }
            }
        }

        return pass;
    }

}
