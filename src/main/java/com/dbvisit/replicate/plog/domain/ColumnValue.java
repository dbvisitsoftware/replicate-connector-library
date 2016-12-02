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

import javax.sql.rowset.serial.SerialBlob;

import com.dbvisit.replicate.plog.format.decoder.DataDecoder;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;

/** 
 * Column value for single change record
 */
public class ColumnValue {
    /** Column ID, the ordinal number of column in parent */ 
    private int            id;
    /** The column data type */
    private ColumnDataType type;
    /** Name of the column */
    private String         name;
    /** Type agnostic column value as decoded */
    @JsonSerialize(using = ToStringSerializer.class)
    private Object         value;
    
    /** Offset of a LOB part in multi-part LOB */
    @JsonIgnore
    private long lobOffset;
    /** Length of a part of LOB in mult-part LOB */
    @JsonIgnore
    private long lobLength;
    /** Position of LOB part within multi-part LOB */
    @JsonIgnore
    private int  lobPosition;
    
    /** Whether or not this column is part of supplementally logged key */
    @JsonIgnore
    private boolean isKey = false;
    
    /**
     * Create empty column value
     */
    public ColumnValue() {}
    
    /**
     * Create and populate a complete column value
     * 
     * @param id    Column ordinal number, ID
     * @param type  Column data type
     * @param name  Column name
     * @param value Decoded column value, as Java type
     */
    public ColumnValue(
        int            id,
        ColumnDataType type,
        String         name,
        Object         value
    ) {
        this.id    = id;
        this.type  = type;
        this.name  = name;
        this.value = value;
    }
    
    /**
     * Set the ID (ordinal number) of column for value
     * 
     * @param id the column ordinal number
     */
    public void setId (int id) {
        this.id = id;
    }
    
    /**
     * Return the ID of the column value, column ordinal number
     * 
     * @return ID
     */
    public int getId () {
        return this.id;
    }
    
    /**
     * Set the data type for column value
     * 
     * @param type Column data type
     */
    public void setType (ColumnDataType type) {
        this.type = type;
    }

    /**
     * Return the data type of the value
     * 
     * @return Column data type of value
     */
    public ColumnDataType getType () {
        return this.type;
    }
    
    /**
     * Set the name of the column for value 
     * 
     * @param name Column name
     */
    public void setName (String name) {
        this.name = name;
    }

    /**
     * Return name of column of data value
     * 
     * @return name of column
     */
    public String getName () {
        return this.name;
    }
    
    /**
     * Return the safe name of the column value, for now it only
     * removes strange characters
     * 
     * @return name of column
     */
    @JsonIgnore
    public String getSafeName () {
        String safename = null;
        
        if (name != null) {
            safename = name.replace("$", "");
        }
        
        return safename;
    }

    /** 
     * Set the value for column, as type agnostic value
     * 
     * @param value decoded value for column
     */
    public void setValue (Object value) {
        this.value = value;
    }

    /**
     * Return the value for this column as type agnostic object
     * 
     * @return value object
     */
    public Object getValue () {
        return this.value;
    }

    /**
     * Set the offset of LOB part in parent LOB
     * 
     * @param lobOffset byte offset, long integer
     */
    public void setLobOffset (long lobOffset) {
        this.lobOffset = lobOffset;
    }

    /**
     * Return the offset of LOB part in parent LOB
     * 
     * @return byte offset within parent, as long
     */
    public long getLobOffset () {
        return this.lobOffset;
    }

    /**
     * Set the length of the LOB
     * 
     * @param lobLength length, as long
     */
    public void setLobLength (long lobLength) {
        this.lobLength = lobLength;
    }

    /**
     * Return length of LOB
     * 
     * @return length of LOB as long integer
     */
    public long getLobLength () {
        return this.lobLength;
    }

    /**
     * Set the index position of the LOB part in parent LOB
     * 
     * @param lobPosition position index
     */
    public void setLobPosition (int lobPosition) {
        this.lobPosition = lobPosition;
    }

    /**
     * Return the position of the LOB part in parent LOB
     * 
     * @return position of LOB as integer
     */
    public int getLobPosition () {
        return this.lobPosition;
    }
    
    /**
     * Set whether or not this column is part of supplemental key
     * 
     * @param isKey true if part of key, else false
     */
    public void setIsKey (boolean isKey) {
        this.isKey = isKey;
    }
    
    /**
     * Return whether or not this column value is part of a supplementally
     * logged key
     * 
     * @return true if part of supplemental key, else false
     */
    @JsonIgnore
    public boolean isKey () {
        return this.isKey;
    }
    
    /** Value as string for logging
     * 
     * @return String value of object for logging
     */
    @JsonProperty ("value")
    public String getValueAsString () {
        String str = null;
        if (value != null) {
            if (value instanceof SerialBlob) {
                SerialBlob sb = (SerialBlob)value;
            
                try {
                    str = 
                        DataDecoder.bytesToHex(
                            sb.getBytes(1, (int)sb.length())
                        );
                } catch (Exception e) {
                    /* just use the normal string representation */
                    str = sb.toString();
                }
            }
            else {
                str = value.toString();
            }
        }
        return str;
    }
    
    /**
     * Plain string representation for column value
     * 
     * @return column value as string
     */
    public String toString () {
        return "Column " + id + " name: " + name + " type: " + type +
               " value: " + getValueAsString();
    }
    
}
