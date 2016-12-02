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

import java.util.HashMap;
import java.util.Map;

/**
 * PLOG entry and sub type used to identify the contents of the PLOG entry
 */
public enum EntrySubType {
	/** PLOG file header */
    ESTYPE_HEADER (
        EntryType.ETYPE_CONTROL, 0, "header"
    ),
    /** PLOG file footer */
    ESTYPE_FOOTER (
        EntryType.ETYPE_CONTROL, 1, "footer"
    ),
    /** The start of a transaction */
    ESTYPE_TRAN_START (
        EntryType.ETYPE_TRANSACTIONS, 1, "start"
    ),
    /** The end of a transaction */
    ESTYPE_TRAN_COMMIT (
        EntryType.ETYPE_TRANSACTIONS, 4, "commit"
    ),
    /** The changes in a transaction have been rolled back */
    ESTYPE_TRAN_ROLLBACK (
        EntryType.ETYPE_TRANSACTIONS, 5, "rollback"
    ),
    /** The changes in a transaction since last save point have been rolled back */
    ESTYPE_TRAN_ROLLBACK_TO_SAVEPOINT (
        EntryType.ETYPE_TRANSACTIONS, 6, "rollback to savepoint"
    ),
    /** An audit transaction */
    ESTYPE_TRAN_AUDIT (
        EntryType.ETYPE_TRANSACTIONS, 7, "audit"
    ),
    /** New data */
    ESTYPE_LCR_INSERT (
        EntryType.ETYPE_LCR_DATA, 2, "insert"
    ),
    /** Existing data have been removed */
    ESTYPE_LCR_DELETE (
        EntryType.ETYPE_LCR_DATA, 3, "delete"
    ),
    /** Existing data have been updated */
    ESTYPE_LCR_UPDATE (
        EntryType.ETYPE_LCR_DATA, 5, "update"
    ),
    /** New LOB */
    ESTYPE_LCR_LOB_WRITE (
        EntryType.ETYPE_LCR_DATA, 16, "LOB write"
    ),
    /** Existing LOB has been trimmed */
    ESTYPE_LCR_LOB_TRIM (
        EntryType.ETYPE_LCR_DATA, 17, "LOB trim"
    ),
    /** Existing LOB has been removed */
    ESTYPE_LCR_LOB_ERASE (
        EntryType.ETYPE_LCR_DATA, 18, "LOB erase"
    ),
    /** Data definition change */
    ESTYPE_LCR_DDL (
        EntryType.ETYPE_LCR_DATA, 256, "DDL"
    ),
    /** No operation change, these are associated with meta data */
    ESTYPE_LCR_NOOP (
        EntryType.ETYPE_LCR_DATA, 512, "no operation"
    ),
    /** Include another PLOG file to load */
    ESTYPE_LCR_PLOG_IFILE (
        EntryType.ETYPE_LCR_PLOG, 1, "include plog file"
    ),
    /** Row statistics for the previously included PLOG file to laod */
    ESTYPE_LCR_PLOG_IFILE_STATS (
        EntryType.ETYPE_LCR_PLOG, 2, "include plog file - rowcount"
    ),
    /** The data definition for a replicated schema encoded in JSON */
    ESTYPE_DDL_JSON (
        EntryType.ETYPE_METADATA, 1, "table DDL as JSON"
    ),
    /** Unknown and unsupported entry record type */
    ESTYPE_UNKNOWN (
        EntryType.ETYPE_UNKNOWN, 9999, "?"
    );

	/** The parent type or category for this sub type record */
    private final EntryType parent;
    /** The sub type ID, used as type ID */
    private final int id;
    /** A human friendly description */
    private final String description;
    /** Direct index lookup */
    private static final Map<Long, EntrySubType> index = 
        new HashMap<Long, EntrySubType>();

    /**
     * Return the ID of this type record 
     * 
     * @return type ID, originally the sub type ID of PLOG entry record
     */
    public int getId () {
        return this.id;
    }

    /**
     * Return the description for this type record
     * 
     * @return the description of this type
     */
    public String getDescription () {
        return this.description;
    }

    /**
     * Return the parent type for this type record
     * 
     * @return parent type record
     * 
     * @see EntryType
     */
    public EntryType getParent () {
        return this.parent;
    }

    /**
     * Return a human friendly string version of this entry type record
     * 
     * @return parent and child type description, as string
     */
    @Override
    public String toString() {
        return parent.toString() + "/" + description;
    }

    /**
     * Create a new entry type record from parent type, the type ID and
     * description
     * 
     * @param parent      The parent PLOG entry record type for this type
     * @param id          The type ID of this record, the sub type ID
     * @param description The description of this type
     */
    private EntrySubType (EntryType parent, int id, String description) {
        this.parent = parent;
        this.id = id;
        this.description = description;
    }

    /**
     * Instance method for building a static index of all defined enum types
     * at run time to allow for efficient lookups
     */
    public static void buildIndex () {
        for (EntrySubType type : EntrySubType.values()) {
            index.put (
                getIndexKey(type.getParent().getId(), type.getId()), 
                type
            );
        }
    }

    /**
     * Build the index key to use for a parent type ID and child (sub) type
     * ID.
     * 
     * @param parentId The ID of parent category or type
     * @param id       The ID of child or sub type
     * 
     * @return composite key value from parent and child type
     */
    private static Long getIndexKey (int parentId, int id) {
        return new Long (((long)parentId << 32) | (long)id);
    }

    /** 
     * Lookup EntrySubType by tag value
     * 
     * @param parentId The parent type ID for the sub type to find
     * @param id       The sub type ID in PLOG format to find
     * 
     * @return EntrySubType
     * @throws Exception If no record is found
     */
    public EntrySubType find (int parentId, int id) throws Exception {
        EntrySubType type = null;

        if (index.size() == 0) {
            buildIndex();
        }

        Long idx = getIndexKey(parentId, id);
        if (index.containsKey(idx)) {
            type = index.get(idx);
        }
        else {
            throw new Exception (
                "No entry sub type found for parent type: " + parentId +
                " and child type: " + id
            );
        }

        return type;
    }

}
