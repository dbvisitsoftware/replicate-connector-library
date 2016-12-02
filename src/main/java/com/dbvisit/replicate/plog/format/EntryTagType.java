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
 * Definition of supported types of PLOG entry tag records.
 */
public enum EntryTagType {
    /* 0: general */
    /** Size of PLOG file */    
    TAG_FILESIZE(0x00000001, "Size of PLOG file"),
    /** Checksum of PLOG */     
    TAG_FILE_CHECKSUM(0x00000002, "Checksum of PLOG"),
    /** Start of footer entry, counted from end of PLOG file */ 
    TAG_FOOTER_START(0x00000003, "Offset from end of PLOG where footer start"),
    /** Define algorithm used for checksums */   
    TAG_CHECKSUM_ALG(0x00000004, "Checksum algorithm"),
    /** Footer comment */
    TAG_FOOTER_COMMENT(0x00000005, "PLOG footer comment"),
    /** Universally unique identifier (UUID) of MINE */    
    TAG_MINE_UUID(0x00000006, "Unique ID for MINE process"),
    /** PLOG sequence starting from 1 */    
    TAG_PLOGSEQ(0x00000007, "PLOG sequence number"),
    /** Bitfields of enabled features */    
    TAG_FEATURES_1(0x00000008, "Enabled features bitset"),
    /** Entry record checksum */      
    TAG_CHECKSUM(0x00000009, "Checksum for PLOG entry"),
    /** PLOG name */      
    TAG_PLOGNAME(0x00000010, "PLOG name"),
    /** PLOG filename */      
    TAG_PLOG_FILENAME(0x00000011, "PLOG filename"),
    /** Name of apply that should parse this */ 
    TAG_APPLY_NAME(0x00000012, "Apply name"),
    /** Number of rows in an included PLOG file */ 
    TAG_ROW_COUNT(0x00000013, "Number of rows"),

    /* 1: database info */
    /** Source database ID */   
    TAG_DBID(0x00010001, "Source database ID"),
    /** Thread and sequence number */  
    TAG_THREADSEQ(0x00010002, "Thread and sequence number"),
    /** Start SCN */      
    TAG_LOWSCN(0x00010003, "Start SCN"),
    /** End SCN */      
    TAG_HIGHSCN(0x00010004, "End SCN"),

    /* 2: column data */
    /** Old column values */
    TAG_PREIMAGE(0x00020001, "Old column values"),
    /** New column values */
    TAG_POSTIMAGE(0x00020002, "New column values"),
    /** Key (supplemental logging) column values */
    TAG_KEYIMAGE(0x00020003, "Supplemental logging key"),
    /** LOB data */
    TAG_LOBDATA(0x00020004, "LOB data"),
    /** LOB offset - where to start */ 
    TAG_LOBOFFSET(0x00020005, "LOB offset"),
    /** LOB position */     
    TAG_LOB_POSITION(0x00020006, "LOB position"),
    /** CDC column not used in plog, only internally in in apply */
    TAG_CDC(0x00020007, "Internal CDC column"),

    /** LOB length */   
    TAG_LOBLEN(0x00020007, "LOB length"),
    /** LOB locator */     
    TAG_LOBLOCATOR(0x00020008, "LOB locator"),
    /** Column ID, ordinal number */      
    TAG_COL_ID(0x00020010, "Column ID"),
    /** Column name */      
    TAG_COL_NAME(0x00020011, "Column name"),
    /** Column type */ 
    TAG_COL_TYPE(0x00020012, "Column type"),
    /** Object ID */      
    TAG_OBJ_ID(0x00020020, "Object ID"),
    /** Object name */      
    TAG_OBJ_NAME(0x00020021, "Object name"),
    /** Object owner */      
    TAG_OBJ_OWNER(0x00020022, "Object owner"),
    /** Base object id */     
    TAG_BASEOBJ_ID(0x00020023, "Base object ID"),
    /** LCR sequence id */     
    TAG_LCR_ID(0x00020030, "LCR sequence ID"),
    /** LCR sequence id (for rollback to savepoint) */
    TAG_SAVEPOINT_ID(0x00020031, "Savepoint ID"),
    /** Redo Block Address */     
    TAG_RBA(0x00020032, "Redo Block Address"),
    /** Describe old columns and null/not null */
    TAG_COLUMN_SIGNATURE_PRE(0x00200101, "Old column signature"),
    /** Describe new columns and null/not null */
    TAG_COLUMN_SIGNATURE_POST(0x00200102, "New column signature"),
    /** Describe key columns */  
    TAG_COLUMN_SIGNATURE_KEY(0x00200103, "Column signature key"),

    /* 3: transaction management */
    /** Transaction ID of current transaction */   
    TAG_XID(0x00030001, "Transaction ID"),
    /** SCN of current operation */    
    TAG_SCN(0x00030002, "SCN of current operation"),
    /** Time of current operation */    
    TAG_DTIME(0x00030003, "Time of current operation"),
    /** Session ID */      
    TAG_AUDIT_SID(0x00030010, "Session ID"),
    /** Session serial number */      
    TAG_AUDIT_SERIAL(0x00030011, "Session serial number"),
    /** Current user name */      
    TAG_AUDIT_CUSER(0x00030012, "Current user name"),
    /** Logon user name */      
    TAG_AUDIT_LUSER(0x00030013, "User name used to log on"),
    /** Client information */      
    TAG_AUDIT_CLIINFO(0x00030014, "CLI client information"),
    /** OS user name */      
    TAG_AUDIT_OSUSER(0x00030015, "OS user name"),
    /** Machine name */      
    TAG_AUDIT_MACHINE(0x00030016, "Host machine name"),
    /** OS terminal */      
    TAG_AUDIT_OSTERM(0x00030017, "OS terminal"),
    /** OS process ID */     
    TAG_AUDIT_OSPROC(0x00030018, "OS process ID"),
    /** OS program name */     
    TAG_AUDIT_OSPROG(0x00030019, "OS program name"),
    /** Transaction name of current transaction */   
    TAG_XID_NAME(0x0003001a, "Transaction ID name"),
    
    /* x10: DDL, zero terminated strings */
    /** Logon schema */
    TAG_LOGON_SCHEMA(0x0040001, "Name of schema used to log on"),
    /** Current schema */
    TAG_CURRENT_SCHEMA(0x0040002, "Name of current schema"),
    /** SQL Text */
    TAG_SQL_TEXT(0x0040003, "SQL text"),
    /** DDL SQL operation */
    TAG_DDL_SQLOP(0x0040004, "DDL SQL operation"),
    /** JSON text */
    TAG_JSON_TEXT(0x0040005, "JSON text"),
    /** Unknown and unsupported tag in PLOG */
    TAG_UNKNOWN(0xFFFFFF, "Unknown PLOG tag");

    /** ID of tag type */
    private final int id;
    /** Human friendly description of PLOG entry tag type */
    private final String description;
    /** Static index of types used for efficient lookup at run time */
    private static final Map<Integer, EntryTagType> index =
        new HashMap<Integer, EntryTagType>();

    /**
     * Return the type ID of this tag type record
     * 
     * @return type ID value
     */
    public int getId () {
        return this.id;
    }

    /**
     * Return the description for this type of tag
     * 
     * @return human friendly description of tag type
     */
    public String getDescription () {
        return this.description;
    }

    /**
     * Returns the description of tag as textual representation of the tag
     * type record
     * 
     * @return string representation of the tag type record
     */
    @Override
    public String toString() {
        return this.description;
    }

    /**
     * Create a entry tag enum entry from type ID and description
     * 
     * @param id          type ID
     * @param description type description
     */
    private EntryTagType (int id, String description) {
        this.id = id;
        this.description = description;
    }

    /**
     * Instance method to prepare a index to lookup enum entry for tag type
     * by decoded tag ID
     */
    private static void buildIndex () {
        for (EntryTagType type : EntryTagType.values()) {
            index.put (type.getId(), type);                
        }
    }

    /**
     * Lookup tag type record by decoded tag ID, using pre-build index
     * 
     * @param id type ID
     * 
     * @return entry tag type record
     * @throws Exception when no tag record for this type ID
     */
    public EntryTagType find (int id) throws Exception {
        EntryTagType type = null;
        
        if (index.size () == 0) {
            buildIndex ();
        }
        
        if (index.containsKey (id)) {
            type = index.get (id);
        }
        else {
            throw new Exception ("No entry tag type found for type: " + id);
        }
        
        return type;
    }
    
    
    /**
     * Define the entry tag types for encoding the features present in a 
     * PLOG file
     */
    public enum EntryFeature {
        /** 
         * Plog compression - do not put any column name/type twice into the 
         * same plog 
         */
        TAG_FEATURES_1_CACHE_DICT (
            0x00000001, 
            "PLOG compression is enabled"
        ),

        /** plog dictionary in JSON format */
        TAG_FEATURES_1_JSON_DICT (
            0x00000002, 
            "PLOG dictionary in JSON format"
        ),

        /** 
         * Plog contains only committed changes, no rollbacks or rollbacks
         * to savepoint
         */
        TAG_FEATURES_1_PESSIMISTIC_COMMIT (
            0x00000004, 
            "PLOG contains only committed changes"
        ),

        /** 
         * Transactions do not overlap; on the other hand, it means that SCN 
         * order is preserved only in a transaction
         */
        TAG_FEATURES_1_SERIALIZED_TXS (
            0x00000008,
            "PLOG contains serialized transactions"
        );

        /** Feature type tag ID */
        private final int id;
        /** Description of PLOG feature */
        private final String description;

        /**
         * Return the feature tag ID
         * 
         * @return type ID for feature tag
         */
        public int getId () {
            return this.id;
        }
 
        /**
         * Return the description for the feature tag type
         * 
         * @return human friendly description for the feature tag in header
         */
        public String getDescription () {
            return this.description;
        }

        /**
         * Convert the feature entry type as string
         * 
         * @return the human friendly description for this feature type
         */
        @Override
        public String toString() {
            return this.description;
        }

        /**
         * Create new entry feature type from feature type tag ID and
         * human friendly description
         * 
         * @param id          type ID for feature tag
         * @param description description for encoded feature
         */
        private EntryFeature (int id, String description) {
            this.id = id;
            this.description = description;
        }
    }

    /**
     * Define the types of LOB entry tag types available in PLOG
     */
    public enum EntryTagLOB {
    	/** Single part LOB */
        LOB_ONE_PIECE(1, "LOB in one part"),
        /** First part of a multi-part LOB */
        LOB_FIRST_PIECE(2, "First part of LOB"),
        /** The next part of a multi-part LOB */ 
        LOB_NEXT_PIECE(3, "Next part of LOB"),
        /** The final part of a multi-part LOB */
        LOB_LAST_PIECE(4, "Last part of LOB");

        /** The LOB part tag type ID */
        private final int id;
        /** Description of the LOB tag type */
        private final String description;

        /**
         * Return the ID of the LOB tag type 
         * 
         * @return LOB part tag type ID 
         */
        public int getId () {
            return this.id;
        }

        /**
         * Return the description of the LOB part tag
         * 
         * @return meaning of the LOB part tag
         */
        public String getDescription () {
            return this.description;
        }

        /**
         * Return the description of the LOB part tag as the textual
         * representation of this type record
         * 
         * @return string representation of type record
         */
        @Override
        public String toString() {
            return this.description;
        }

        /**
         * Create a LOB part type record from type ID and description
         * 
         * @param id          the ID of LOB part type
         * @param description the description of its meaning
         */
        private EntryTagLOB (int id, String description) {
            this.id = id;
            this.description = description;
        }
    }

    /**
     * The column signature entry tag types
     */
    public enum EntryTagSignature {
        /** Signature is present, lower 10 bits is column number */  
        SIGNATURE_HAS_DATA(0x1000, "Signature is present"),
        /** Column signature is not present */
        SIGNATURE_NOT_PRESENT(0x000, "Signature is not present"),
        /** Signature is empty */
        SIGNATURE_NULL(0x2000, "Signature is empty"),
        /** Signature has data or is empty, for new values does not matter */  
        SIGNATURE_HAS_DATA_OR_NULL(0x3000, "Signature has data or empty");

        /** Signature tag type ID */
        private final int id;
        /** Description of signature tag in PLOG */
        private final String description;

        /**
         * Return the signature tag type ID
         * 
         * @return type ID
         */
        public int getId () {
            return this.id;
        }

        /**
         * Return the description for this signature tag type
         * 
         * @return description of type
         */
        public String getDescription () {
            return this.description;
        }

        /**
         * Return the string version of this signature tag type
         * 
         * @return description of signature tag type
         */
        @Override
        public String toString() {
            return this.description;
        }

        /**
         * Create new signature tag type record from tag ID and description
         * 
         * @param id          Signature tag type ID
         * @param description Description for signature tag type
         */
        private EntryTagSignature (int id, String description) {
            this.id = id;
            this.description = description;
        }
    }

}
