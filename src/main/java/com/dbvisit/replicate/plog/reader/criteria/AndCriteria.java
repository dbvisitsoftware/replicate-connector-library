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
 * And criteria that consist of two criteria that must both have their
 * criteria met to pass
 * 
 * @param <T> Type parameter for filter
 */
public class AndCriteria<T> implements Criteria<T> {
    /** First criteria in AND to apply to a <em>FilterableRecord</em> */
    Criteria<T> c1;
    /** Second criteria in AND to apply to a <em>FilterableRecord</em> */
    Criteria<T> c2;

    /**
     * Create an AND criteria from two criteria, both of which must have
     * their requirements met for a record to have it pass the AND criteria
     * 
     * @param c1 first criteria in AND to apply to a <em>FilterableRecord</em>
     * @param c2 second criteria in AND to apply to a <em>FilterableRecord</em>
     */
    public AndCriteria (Criteria<T> c1, Criteria<T> c2) {
        this.c1 = c1;
        this.c2 = c2;
    }

    /**
     * Test whether or not a <em>FilterableRecord</em> meets both criteria
     * defined in this AND criteria.
     * 
     * @param rec a record that implements a <em>FilterableRecord</em> and 
     *            has the correct testable properties
     *
     * @return true if the incoming record meets both defined criteria, else
     *         false if it fails any of the two
     */
    @Override
    public boolean meetCriteria (FilterableRecord<T> rec)
    throws Exception {
        boolean pass = false;

        if (c1 == null || c2 == null) {
            throw new Exception ("Invalid AND criteria");
        }

        pass = c1.meetCriteria (rec);

        if (pass) {
            pass = c2.meetCriteria (rec);
        }

        return pass;
    }

}
