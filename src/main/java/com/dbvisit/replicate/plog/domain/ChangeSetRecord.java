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

import java.util.LinkedList;
import java.util.List;

import com.dbvisit.replicate.plog.domain.util.DomainJSONConverter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Change set record as encoded in PLOG
 */
public class ChangeSetRecord extends ChangeRowRecord {
    /** The column values present in a key image tag for PLOG data LCR */
    final private List<ColumnValue> keyValues;
    /** The column values present in an old image tag for PLOG data LCR */
    final private List<ColumnValue> oldValues;
    /** The column values present in a new image tag for PLOG data LCR */
    final private List<ColumnValue> newValues;
    /** The column values present in a LOB image tag for PLOG data LCR */
    final private List<ColumnValue> lobValues;
    
    /**
     * Initialise change set record data sets and record type
     */
    public ChangeSetRecord () {
        keyValues = new LinkedList<ColumnValue>();
        oldValues = new LinkedList<ColumnValue>();
        newValues = new LinkedList<ColumnValue>();
        lobValues = new LinkedList<ColumnValue>();
        recordType = DomainRecordType.CHANGESET_RECORD;
    }
    
    /**
     * Return the key values for a change set record. <p>These are the column
     * values parsed from the <em>TAG_KEYIMAGE</em> tag in an UPDATE or
     * DELETE LCR and is the key column values as supplemental logged
     * for the change action and maye be different from a key constraint.</p>
     *  
     * @return Column values part of supplemental key local to action
     */
    @JsonProperty ("key")
    public List<ColumnValue> getKeyValues () {
        return this.keyValues;
    }
    
    /**
     * Return the old (previous) values for a change set record. <p>These are
     * the old values not affected by change action and in addition the
     * ones affected by change (these will have a counterpart NEW value).</p>
     * 
     * @return Old column values for previous state of change row
     */
    @JsonProperty ("old")
    public List<ColumnValue> getOldValues () {
        return this.oldValues;
    }

    /**
     * Return the new (change) values for a change set record. <p>These will
     * either have a counterpart OLD record in KEY or OLD column list</p>
     * 
     * @return New column values for changed columns
     */
    @JsonProperty ("new")
    public List<ColumnValue> getNewValues () {
        return this.newValues;
    }
    
    /**
     * Return the LOB field values for a change set record. <p>These are
     * emitted as seperate change sets in Oracle REDOs and will always
     * be accompanied with KEY values</p>
     * 
     * @return LOB values for change set (multi-part LCR)
     */
    @JsonProperty ("lob")
    public List<ColumnValue> getLobValues () {
        return this.lobValues;
    }
    
    /**
     * Add a key value identified by supplemental logging for given LCR action
     * 
     * @param keyValue key value (supplemental logging)
     */
    public void addKeyValue (ColumnValue keyValue) {
        keyValues.add (keyValue);
    }
    
    /**
     * Add old value for change set, as in previous state of column
     * 
     * @param oldValue old (previous) value for column
     */
    public void addOldValue (ColumnValue oldValue) {
        oldValues.add (oldValue);
    }
    
    /**
     * Add new value for change set, as in incoming value for a column to
     * replace previous one
     * 
     * @param newValue new value for column
     */
    public void addNewValue (ColumnValue newValue) {
        newValues.add (newValue);
    }
    
    /**
     * Add LOB value for change set
     * 
     * @param lobValue LOB field emitted separately by Oracle for change record
     */
    public void addLobValue (ColumnValue lobValue) {
        lobValues.add (lobValue);
    }
    
    /**
     * Check whether or not the change set has supplemental logged key
     * 
     * @return true of supplemental logged keys are present, else false
     */
    public boolean hasKeyValues () {
        return keyValues.size() > 0;
    }
    
    /**
     * Check whether or not the change set has old (previous) values
     *  
     * @return true if it has old values to write out, else false
     */
    public boolean hasOldValues () {
        return oldValues.size() > 0;
    }
    
    /**
     * Check whether or not the change set has new (incoming) values to
     * replace old column values (the changed fields)
     *  
     * @return true if it has new values for changed columns, else false
     */
    public boolean hasNewValues () {
        return newValues.size() > 0;
    }
    
    /**
     * Check whether or not the change set is for a change involving a
     * LOB field
     * 
     * @return true if LOB fields present in change, else false
     */
    public boolean hasLobValues () {
        return lobValues.size() > 0;
    }
    
    /**
     * Return whether or not this change set has data records, as in valid
     * by checking whether or not it had any keys, old, new or lob values.
     * 
     * @return true if a data record else false
     */
    @Override
    @JsonIgnore
    public boolean isDataRecord() {
        return keyValues.size() > 0 ||
               newValues.size() > 0 ||
               oldValues.size() > 0 ||
               lobValues.size() > 0;
    }
    
    /**
     * De-serialize JSON logical change set record string to domain object
     * 
     * @param json serialized logical change record JSON string
     * 
     * @return logical change record domain object
     * @throws Exception if de-serialization error occur
     */
    public static ChangeSetRecord fromJSONString (String json)
    throws Exception {
        return (ChangeSetRecord)
            DomainJSONConverter.fromJSONString(
                json,
                ChangeSetRecord.class
            );
    }
    
    /**
     * Change sets do not have complete row view of column values, return
     * null instead.
     * 
     * @return null
     */
    @Override
    public List<ColumnValue> getColumnValues () {
        return null;
    }
}
