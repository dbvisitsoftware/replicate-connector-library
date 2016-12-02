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
 * Criteria interface
 * 
 * @param <T> Type parameter for filter
 */
public interface Criteria<T> {
    /**
     * All criteria must implement this method that tests of the incoming
     * filterable record meets the defined criteria
     * 
     * @param record filterable record that can be tested against criteria
     * 
     * @return true if it meets the criteria, else false
     * @throws Exception if the criteria has not been setup correctly
     */
    public boolean meetCriteria (FilterableRecord<T> record) throws Exception;
}
