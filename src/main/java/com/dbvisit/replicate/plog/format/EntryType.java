package com.dbvisit.replicate.plog.format;

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
 * The main categories for PLOG entry record types
 */
public enum EntryType {
    /** PLOG decoding control records category, include header and footer */
    ETYPE_CONTROL (0, "control"),
    /** All transaction records in PLOG */
    ETYPE_TRANSACTIONS (1, "transaction"),
    /** All data records in PLOG */
    ETYPE_LCR_DATA (11, "LCR data"),
    /** Reference to other PLOG files */
    ETYPE_LCR_PLOG (20, "LCR plog"),
    /** Meta data category of replicated objects */
    ETYPE_METADATA (21, "metadata"),
    /** Unknown and unsupported type */
    ETYPE_UNKNOWN (9999, "?");

    /** Entry record category type ID */
    private int id;
    /** Description of entry record type */
    private String description;

    /**
     * Return the ID of entry type category
     * 
     * @return entry type, as ID value encoded in PLOG format
     */
    public int getId () {
        return this.id;
    }

    /**
     * Return description of entry type category
     * 
     * @return description of entry type
     */
    public String getDescription () {
        return this.description;
    }

    /**
     * Return the description of entry type as the string representation
     * of enum entry
     * 
     * @return enum entry as string
     */
    @Override
    public String toString() {
        return description;
    }

    /**
     * Create a new entry type record for entry category type ID and 
     * description
     * 
     * @param id          category ID
     * @param description description of entry type
     */
    private EntryType (int id, String description) {
        this.id = id;
        this.description = description;
    }

}
