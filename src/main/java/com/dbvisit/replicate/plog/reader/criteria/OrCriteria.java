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

/**
 * Or criteria, either of its two criteria may be met for a pass
 * 
 * @param <T> Type parameter for filter
 */
public class OrCriteria<T> implements Criteria<T> {
    /** The first criteria in OR test */
    Criteria<T> c1;
    /** The second criteria in OR test */
    Criteria<T> c2;

    /**
     * Create an OR criteria from two criteria, either of which's requirements
     * may be met for a record to pass the OR test
     * 
     * @param c1 first criteria in OR test
     * @param c2 second criteria in OR test
     */
    public OrCriteria (Criteria<T> c1, Criteria<T> c2) {
        this.c1 = c1;
        this.c2 = c2;
    }

    /**
     * Test to see if incoming <em>FilterableRecord</em> record meets any
     * of the two criteria of the OR criteria
     * 
     * @param rec the filterable record to evaluate against two criteria in
     *            OR test
     *
     * @return true if the records meets one of the OR criteria, else false
     *         if it does not meet any
     */
    @Override
    public boolean meetCriteria (FilterableRecord<T> rec) 
    throws Exception {
        boolean pass = false;

        if (c1 == null || c2 == null) {
            throw new Exception ("Invalid OR criteria");
        }

        if (rec != null) {
            pass = c1.meetCriteria (rec);
            if (!pass) {
                pass = c2.meetCriteria (rec);
            }
        }

        return pass;
    }
}
