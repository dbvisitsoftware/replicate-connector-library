package com.dbvisit.replicate.plog.domain;

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
 * Define types of domain records
 */
public enum DomainRecordType {
    /** A row-level change record, this is a complete record, not a
     *  change vector
     */
    CHANGEROW_RECORD,
    /* A crow-level change set record, this is a change vector */
    CHANGESET_RECORD,
    /** A meta data record for schema definition for change records */
    METADATA_RECORD,
    /** A control header record that holds information about PLOG 
     *  encoding
     */
    HEADER_RECORD,
    /** An aggregate record that holds information about a transaction
     *  proper
     */
    TRANSACTION_INFO_RECORD,
    /** Not a domain record we need */
    NONE;
    
    /**
     * Return string version of name of enum entry
     * 
     * @return formatted name of enum entry
     */
    @Override
    public String toString () {
        return this.name().replaceAll ("_", " ");
    }
    
}
