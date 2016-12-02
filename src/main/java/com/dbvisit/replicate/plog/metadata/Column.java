package com.dbvisit.replicate.plog.metadata;

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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Define the meta data that describes a single column field
 */
public class Column {
    /** Define the start ordinal number of real columns */
    @JsonIgnore
    private static final int ORDINAL_START_ID = 1;
    /** ID of the column, same as ordinal number in source Oracle */
    @JsonProperty("columnId")
    private Integer id;
    /** Name of the column exactly as defined in source Oracle */
    @JsonProperty("columnName")
    private String name;
    /** Oracle data type, as string, for column value */
    @JsonProperty("columnType")
    private String type;
    /** The precision of the data stored in this column */
    @JsonProperty("columnPrecision")
    private Integer precision;
    /** The scale of the data stored in this column */ 
    @JsonProperty("columnScale")
    private Integer scale;
    /** Define whether or not the data in this column may be null */
    @JsonProperty("isNullable")
    private Boolean nullable;

    /** 
     * Create empty, un-initialized column for JSON de-serialization
     */
    public Column () {}


    /**
     * Create and initialize column meta data, used by testing
     * 
     * @param id        column ordinal number, ID
     * @param name      name of column
     * @param type      data type for column
     * @param precision precision for column
     * @param scale     scale for column
     * @param nullable  true if column data may be null, else false if it's
     *                  mandatory
     */
    public Column (
        Integer id,
        String  name,
        String  type,
        Integer precision,
        Integer scale,
        Boolean nullable
    ) {
        this.id        = id;
        this.name      = name;
        this.type      = type;
        this.precision = precision;
        this.scale     = scale;
        this.nullable  = nullable;
    }

    /**
     * Set the ordinal number as defined in the source Oracle table, as the ID
     * of the column
     * 
     * @param id column ID
     */
    public void setId (Integer id) {
        this.id = id;
    }

    /**
     * Return the ordinal number of this column in parent table definition
     * 
     * @return ID of this column definition
     */
    public Integer getId () {
        return this.id;
    }

    /**
     * Set the name of the column as defined in the source Oracle table
     * definition
     * 
     * @param name set name of column
     */
    public void setName (String name) {
        this.name = name;
    }

    /**
     * Return the raw name of the column as defined in the source Oracle, 
     * this may contain <em>$</em> or other strange characters
     * 
     * @return raw column name
     */
    public String getName () {
        return this.name;
    }

    /**
     * Return a version of the column name with all <em>$</em> characters
     * replaced, when a safe version is required by caller
     * 
     * @return column name with no strange characters
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
     * Set the raw Oracle type string for this column as defined in the
     * source database
     * 
     * @param type Oracle type string
     */
    public void setType (String type) {
        this.type = type;
    }

    /**
     * Return the Oracle type string for this column
     * 
     * @return raw type string as defined in Oracle
     */
    public String getType () {
        return this.type;
    }

    /**
     * Set the precision for data stored in this column as defined in the
     * source database as the total number of digits in number types, -1
     * for non-number types
     * 
     * @param precision total number of digits defined in Oracle as precision
     */
    public void setPrecision (Integer precision) {
        this.precision = precision;
    }

    /**
     * Return the total number of digits defined as precision in Oracle
     * data type
     * 
     * @return total number of digits defined in Oracle as precision
     */
    public Integer getPrecision () {
        return this.precision;
    }

    /**
     * Set the number of digits to the right of decimal point as the scale
     * defined for data in this column stored in source Oracle
     * 
     * @param scale number of digits to the right of decimal point
     */
    public void setScale (Integer scale) {
        this.scale = scale;
    }

    /**
     * Return the scale of data stored in this column as defined as the 
     * number of digits to the right of decimal point in Oracle
     * 
     * @return scale of data stored in this column
     */
    public Integer getScale () {
        return this.scale;
    }

    /**
     * Set whether or not data in this column may be missing, eg. set as nulls
     * 
     * @param nullable true if column data may be null, false if mandatory
     */
    public void setNullable (Boolean nullable) {
        this.nullable = nullable;
    }

    /**
     * Return whether or not data in this column may be null or mandatory
     * 
     * @return true if data in column is not mandatory, else true
     */
    public Boolean getNullable () {
        return this.nullable;
    }

    /**
     * Helper function for converting ordinal ID to internal zero-based index
     * 
     * @param columnId ordinal number of column to convert
     * 
     * @return internal zero based index for column ID
     */
    public static int toColumnIdx (int columnId) {
        return columnId - ORDINAL_START_ID;
    }

    /**
     * Helper function to convert internal zero-based column value index
     * for row to column ordinal number
     * 
     * @param idx internal column index
     * 
     * @return ordinal number of column, as ID
     */
    public static int toColumnId (int idx) {
        return idx + ORDINAL_START_ID;
    }

    /**
     * Convert this object as human readable string representation
     * 
     * @return string version of this column meta data
     */
    public String toString () {
        return "Column: " + name + " " + type;
    }

    /** 
     * Simple string representation for easy comparison
     * 
     * @return String representation of column meta data
     */
    public String toCmpString () {
        return id + ":" + name + ":" + type;
    }

}
