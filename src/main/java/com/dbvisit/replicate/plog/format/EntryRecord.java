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

import java.util.List;
import java.util.Map;

import com.dbvisit.replicate.plog.domain.ReplicateOffset;
import com.dbvisit.replicate.plog.file.PlogFile;
import com.dbvisit.replicate.plog.reader.criteria.FilterableRecord;

/**
 * PLOG entry record consisting of raw tag data. This record is filterable 
 * by all parse criteria. Each PLOG entry record consist of a fixed section
 * of 3 chunks, then variable section of chunks containing its tag data.
 */
public class EntryRecord implements FilterableRecord<EntrySubType> {
    /** Entry tag data starts at the 3rd chunk in entry record */
    public static final int DATA_CHUNK_OFFSET = 3;

    /** <em>EntrySubType</em> type, parent and sub type for PLOG entry record */
    private EntrySubType subType;
    /** Length of entry record as encoded in PLOG including its variable data
     *  pay load, encoded as number of PLOG chunks. The length value 
     *  is encoded in the the first chunk of record */
    private int length;
    /** The ID of the parent type of this PLOG entry record, that is the 
     *  main category type for its pay load. This is the second chunk */
    private int typeId;
    /** The ID of the sub type of PLOG entry record pay load, encoded
     *  in the third chunk in PLOG entry record */
    private int subTypeId;
    /** The parsed raw tag data grouped by entry tag type, as encoded in
     *  variable section of PLOG entry record */
    private Map<EntryTagType, List<EntryTagRecord>> entryTags;
    /** Unique offset of this record in a replicate PLOG set */
    private ReplicateOffset offset;
    /** The fully qualified name of the owner of this record within
     *  the replicated stream 
     */
    private String owner;
    /** The Oracle SCN for the change associated with this record */
    private long scn;

    /**
     * Set the length, in number of PLOG chunks, as decoded from the first
     * chunk in record
     * 
     * @param length number of PLOG chunks as the encoded size of this record 
     */
    public void setLength (int length) {
        this.length = length;
    }

    /**
     * Return the encoded size of this record in the PLOG, as length defined
     * by number of PLOG chunks
     * 
     * @return length in number of PLOG chunks
     */
    public int getLength () {
        return this.length;
    }

    /**
     * Set the parent category type ID of this record, as decoded from the
     * second chunk in the PLOG record
     * 
     * @param typeId the main type of this record, as ID
     */
    public void setTypeId (int typeId) {
        this.typeId = typeId;
    }

    /**
     * Return the ID of the parent category of this record
     * 
     * @return main type of this record, as ID
     */
    public int getTypeId () {
        return this.typeId;
    }

    /**
     * Set the sub category type ID of this record, as decoded from the
     * third chunk in the PLOG record
     * 
     * @param subTypeId the sub type of this record and its pay load
     *                  of data tags, as ID value
     */
    public void setSubTypeId (int subTypeId) {
        this.subTypeId = subTypeId;
    }

    /**
     * Return the sub type of this record
     * 
     * @return sub type of this record, as ID
     */
    public int getSubTypeId () {
        return this.subTypeId;
    }

    /** 
     * Set the raw data tags decoded for this record as its data pay load.
     * This is grouped by type of encoded change data to allow quick lookup 
     * of list of data tags per type
     * 
     * @param tags list of raw tag records decoded from variable section in
     *             this PLOG entry record, group by its type
     */
    public void setEntryTags (Map<EntryTagType, List<EntryTagRecord>> tags) {
        this.entryTags = tags;
    }

    /**
     * Return the decoded list of data tags grouped by type of encoded data.
     * 
     * @return map of lists of raw tag records decoded from variable pay load
     *         section in PLOG entry record
     */
    public Map<EntryTagType, List<EntryTagRecord>> getEntryTags () {
        return this.entryTags;
    }

    /**
     * Set the composite type used to identify the type of record and parsing
     * behavior.
     * 
     * @param subType the composite sub type, consisting of parent type and 
     *                sub type
     *
     * @see EntrySubType
     */
    public void setSubType (EntrySubType subType) {
        this.subType = subType;
    }

    /**
     * Return the composite type used to identify this type of record
     * 
     * @return enum entry in <em>EntrySubType</em> as this record's type
     */
    public EntrySubType getSubType () {
        return this.subType;
    }

    /** 
     * Set the replicate offset for this record, which is unique per
     * replicated PLOG stream and is composed of PLOG sequence and
     * record file offset within PLOG. This is required for offset
     * based criteria used as parse filter.
     * 
     * @param offset the unique offset of record within replicate
     *               stream, sequence of PLOGs
     */
    public void setUniqueOffset (ReplicateOffset offset) {
        this.offset = offset;
    }

    /**
     * Set the full identifier for the owner of this record, as in the
     * parent of this change. This is required for schema or owner name
     * based criteria used as parse filter.
     * 
     * @param owner full owner identifier, for table name it is qualified
     *              with table owner or schema name
     */
    public void setOwner (String owner) {
        this.owner = owner;
    }

    /**
     * Set the Oracle change number for this PLOG entry record. This is
     * required for SCN based criteria used as parse filter.
     * 
     * @param scn the Oracle SCN for this change entry record
     */
    public void setSCN (long scn) {
        this.scn = scn;
    }

    /**
     * Reset the internal state of this record and clear out all references
     * to raw data tags.
     */
    public void clear () {
        if (entryTags != null) {
            for (List<EntryTagRecord> tags : entryTags.values()) {
                for (EntryTagRecord tag : tags) {
                    tag.clear();
                }
                tags.clear();
            }
            entryTags.clear();
        }
        length = 0;
        subType = EntrySubType.ESTYPE_UNKNOWN;
        typeId = -1;
        subTypeId = -1;
    }

    /** 
     * Return size in bytes of this PLOG record
     * 
     * @return int size of PLOG entry in bytes
     */
    public int getSize () {
        return this.length * PlogFile.PLOG_DATA_CHUNK_BYTES;
    }

    /**
     * Return whether or not this PLOG entry record is a control header
     * record which holds the data encoding and feature set information.
     * This is usually the first entry record, after the PLOG file header,
     * in the PLOG.
     * 
     * @return true if this record has the type of control header, else false
     */
    public boolean isHeader () {
        return (subType.equals (EntrySubType.ESTYPE_HEADER));
    }

    /**
     * Check whether or not this is the footer record which ends the PLOG.
     * This is always the last record in PLOG and all domain parsers
     * will keep parsing the PLOG stream until this record ends it
     * 
     * @return true if this is footer of the PLOG, the last record that ends
     *         the PLOG, else false
     */
    public boolean isFooter () {
        return (subType.equals (EntrySubType.ESTYPE_FOOTER));
    }

    /**
     * Check whether or not this record contains a data definition language
     * statement as SQL
     * 
     * @return true if this record contains DDL SQL, else false
     */
    public boolean isDDL () {
        return (subType.equals (EntrySubType.ESTYPE_LCR_DDL));
    }

    /**
     * Check whether or not this entry record contains the definition of
     * a replicated schema as JSON. This defines the structure of the
     * source table from a the SCN at which a DDL operation was done, 
     * either creating or modifying an existing source
     * 
     * @return true if this record holds the JSON meta data needed to define
     *         a replicated schema in PLOG, else false
     */
    public boolean isJSONMetaData () {
        return (subType.equals (EntrySubType.ESTYPE_DDL_JSON));
    }

    /**
     * Return whether or not this record is a change record affecting
     * a LOB field. LOBs are emitted as separate change records and if
     * needed must be merged with it's non-LOB field counterpart LCR
     * for certain targets. This type of record is considered to be a 
     * partial LCR for certain domain parsers.
     * 
     * @return true if a partial record containing only LOB fields, else
     *         false
     */
    public boolean isLOB () {
        return this.subType.equals (EntrySubType.ESTYPE_LCR_LOB_WRITE);
    }

    /**
     * Check whether or not this PLOG entry record is a transaction record.
     * All data LCRs are committed and their transactions are implicit,
     * however certain aggregate domain parsers need to parse all 
     * transactions including AUDIT ones. Rollbacks are never acted on.
     * 
     * @return true if this record holds transaction details, else false
     */
    public boolean isTransaction () {
        return this.subType.getParent().equals(EntryType.ETYPE_TRANSACTIONS);
    }

    /**
     * Check if entry is a data change record, but not a No-Operation 
     * or DDL record
     * 
     * @return true if is of the correct type to have column date, else false
     */
    public boolean hasColumnData () {
        return (
            subType.getParent().equals (EntryType.ETYPE_LCR_DATA) &&
            !subType.equals (EntrySubType.ESTYPE_LCR_NOOP) &&
            !subType.equals (EntrySubType.ESTYPE_LCR_DDL)
        );
    }

    /**
     * Check if this entry record is for a change action on column data, 
     * including INSERTS, UPDATES, DELETES, LOB WRITES, NO OPERATIONS and DDL. 
     * All of these may have column meta data fields encoded, however for
     * compact PLOGs these will only be present in first record of this
     * type in each PLOG. For verify that the actual LCR contains the
     * correct encoded fields see <em>canParseColumnMetaData()</em>
     * 
     * @return true if LCR may contain fields to parse for column meta data,
     *         needed when building table dictionary, else false
     */
    public boolean useColumnMetaData () {
        return (subType.equals (EntrySubType.ESTYPE_LCR_INSERT) ||
                subType.equals (EntrySubType.ESTYPE_LCR_UPDATE) ||
                subType.equals (EntrySubType.ESTYPE_LCR_DELETE) ||
                subType.equals (EntrySubType.ESTYPE_LCR_LOB_WRITE) ||
                subType.equals (EntrySubType.ESTYPE_LCR_NOOP) ||
                subType.equals (EntrySubType.ESTYPE_LCR_DDL));
    }

    /** 
     * Return whether or not this record actually has the correct fields
     * encoded in tag pay load to be parsed as column meta data for the
     * table dictionary, including column ID, name and data type as 
     * defined in source Oracle.
     * 
     * @return true if this record contains column meta data fields to parse
     *         as the table dictionary, else false
     */
    public boolean canParseColumnMetaData () {
        return entryTags.containsKey(EntryTagType.TAG_COL_ID) &&
               entryTags.containsKey(EntryTagType.TAG_COL_NAME) &&
               entryTags.containsKey(EntryTagType.TAG_COL_TYPE);
    }

    /**
     * Check if this PLOG entry record is a reference to another PLOG
     * file to include and process at this record's SCN. These are used 
     * for processing and loading LOAD PLOGs, when this record is parsed
     * by domain parser it effectively pauses the parser and defers 
     * processing to a proxy, which emits each record in LOAD PLOG as if
     * it was present in this record's parent PLOG.
     * 
     * @return true if this record includes reference to LOAD PLOG to load
     *         at this records SCN, else false
     */
    public boolean isIncludeFileRecord () {
        return subType.equals(EntrySubType.ESTYPE_LCR_PLOG_IFILE);
    }

    /**
     * Check if this record contains the row count for the referenced
     * LOAD PLOG in its include file counterpart, usually the previous 
     * record. The number of rows is used to verify that LOAD was done
     * correctly or to report overall number if already processed.
     * 
     * @return true if this record contains the LOAD file row count, else
     *         false
     */
    public boolean isIncludeFileStatsRecord () {
        return subType.equals(EntrySubType.ESTYPE_LCR_PLOG_IFILE_STATS);
    }

    /**
     * Return the entry record composite type as the record type used for 
     * filtering of PLOG records by type
     * 
     * @return the type of this entry record
     * 
     * @see FilterableRecord
     */
    @Override
    public EntrySubType getRecordType() {
        return this.subType;
    }

    /**
     * Return the unique replicate offset of this entry record as the record
     * offset used for filtering records in PLOG by offset
     * 
     * @return the offset of this record in replicate stream
     * 
     * @see FilterableRecord
     */
    @Override
    public ReplicateOffset getRecordOffset() {
        return this.offset;
    }

    /**
     * Return the identifier of the parent of this change record as the name
     * of the record schema required by filtering of PLOG stream entries by
     * owner
     * 
     * @return name of the schema or owner of this record
     * 
     * @see FilterableRecord
     */
    @Override
    public String getRecordSchema() {
        return this.owner;
    }

    /**
     * Check if this entry record has column data, which indicates that it
     * is a data record, this is required during filtering of stream entries
     * 
     * @return true if this is a data change record, else false
     * 
     * @see FilterableRecord
     */
    @Override
    public boolean isDataRecord() {
        return hasColumnData();
    }

    /** 
     * LOB records are encoded as partial records in PLOG
     * 
     * @return true if LOB write, else false
     */
    public boolean isPartialRecord() {
        return subType.equals (EntrySubType.ESTYPE_LCR_LOB_WRITE) ||
               subType.equals (EntrySubType.ESTYPE_LCR_LOB_TRIM)  ||
               subType.equals (EntrySubType.ESTYPE_LCR_LOB_ERASE);
    }

    /**
     * Return the Oracle SCN for this PLOG entry record required for filtering
     * any record from a PLOG based on source SCN
     * 
     * @return the source SCN value for this record
     * 
     * @see FilterableRecord
     */
    @Override
    public Long getRecordSCN() {
        return scn;
    }
    
}
