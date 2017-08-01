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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Meta data class for data definition changes in PLOG including the definition
 * for columns, as de-serialized from JSON dictionary records in PLOG. These
 * originate from DDL statements in source database
 */
public class DDLMetaData {
    /** Identify DDL meta data from recycling bin by simple prefix only */
    private final static String RECYCLING_BIN_DDL_PREFIX = "BIN";
    /** SCN value at which this data definition change took place */
    private Long         validSinceSCN;
    /** Object ID in Oracle that this data definition change applies to */
    private Integer      objectId;
    /** The parent owner or user schema for Oracle object ID above */
    private String       schemaName;
    /** The name of parent for Oracle object ID above */
    private String       tableName;
    /** Definitions for the column present after data definition change */
    @JsonProperty("columns")
    private List<Column> tableColumns;
    /** Index for finding column meta data by ordinal number or ID */
    @JsonIgnore
    private Map<Integer, Column> columns;
    /** Whether or not the table has key constraints */
    @JsonProperty("hasKey")
    private Boolean hasKey = false;

    /**
     * Set the Oracle SCN number at which this data definition was created 
     * or existing one modified by a DDL statement
     * 
     * @param validSinceSCN the SCN at which the definition took effect
     */
    public void setValidSinceSCN (Long validSinceSCN) {
        this.validSinceSCN = validSinceSCN;
    }

    /**
     * Return the Oracle SCN number at which the data definition took effect
     * or existing one was modified at, this definition is valid since
     * the return SCN value
     * 
     * @return SCN value from which definition is valid
     */
    public Long getValidSinceSCN () {
        return this.validSinceSCN;
    }

    /**
     * Set the object ID in Oracle that is defined by this data definition
     * 
     * @param objectId Oracle object ID for definition
     */
    public void setObjectId (Integer objectId) {
        this.objectId = objectId;
    }

    /**
     * Return the object ID in Oracle that the data definition is valid for,
     * either new definition or it had it's definition modified by a DDL 
     * statement
     * 
     * @return Oracle object ID for definition change
     */
    public Integer getObjectId () {
        return this.objectId;
    }

    /**
     * Set the name of the schema for the data definition record
     * 
     * @param schemaName parent of the object ID for this schema definition
     */
    public void setSchemaName (String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * Return the schema name, as parent of object for the DDL change
     * 
     * @return name of schema or object owner
     */
    public String getSchemaName () {
        return this.schemaName;
    }

    /**
     * Set the name of the table of the incoming data definition change
     * encoded in PLOG JSON record
     * 
     * @param tableName name of table for the data definition change
     */
    public void setTableName (String tableName) {
        this.tableName = tableName;
    }

    /**
     * Return the name of the table for this data definition change
     * 
     * @return name of table that this data definition change was applied to
     */
    public String getTableName () {
        return this.tableName;
    }

    /**
     * Set the list of columns defined by this data definition change
     * record; build the internal column lookup table if needed
     * 
     * @param tableColumns incoming list of columns
     */
    public void setTableColumns (List<Column> tableColumns) {
        this.tableColumns = tableColumns;

        if (columns == null) {
            columns = new HashMap<Integer, Column>();
        }

        if (columns.size() != tableColumns.size()) {
            for (Column column : tableColumns) {
                columns.put (column.getId(), column);
            }
        }
    }

    /**
     * Return the list of columns in this data definition change sorted
     * by ordinal number, to lookup directly by column ID use helper
     * function <em>Column.toColumnIdx(ID)</em>
     * 
     * @return list of columns in the order as defined in source database
     */
    public List<Column> getTableColumns () {
        return this.tableColumns;
    }

    /**
     * Set the index to use for finding column meta data by ordinal number,
     * column ID, usually it's build internally, but when external updates
     * to meta data is trigger by table row dictionary changes we need
     * to manually update the column index
     * 
     * @param columns lookup of column meta data by ID
     */
    @JsonIgnore
    public void setColumns (Map<Integer, Column> columns) {
        this.columns = columns;
    }

    /**
     * Return the index for finding column meta data by column ID or
     * encoded ordinal number
     * 
     * @return lookup of column meta data by ID
     */
    public Map<Integer, Column> getColumns () {
        return this.columns;
    }

    /**
     * Check whether the meta data for this data definition change is
     * valid, as in all the required ones are present and contain
     * data
     * 
     * @return true if all information is present, else false
     */
    public boolean isValid() {
        boolean valid = false;

        if (schemaName != null   && schemaName.length() > 0 &&
            tableName != null    && tableName.length() > 0  &&
            tableColumns != null && tableColumns.size() > 0)
        {
            valid = true;
        }
        return valid;
    }

    /**
     * Return full schemata name, with all special characters removed, only
     * for valid data definition change record
     * 
     * @return String containing fully qualified schema name
     */
    public String getSchemataName () {
        String schemata = null;

        if (isValid()) {
            schemata = 
                schemaName.replace("$", "") + "." + 
                tableName.replace("$", "");
        }

        return schemata;
    }

    /**
     * Since Oracle 10g, dropped tables are put in the recycle bin
     * 
     * @return true if table name shows the table came from recycling bin
     */
    @JsonIgnore
    public boolean fromRecycleBin () {
        return tableName.startsWith(RECYCLING_BIN_DDL_PREFIX);
    }

    /**
     * Set whether or not this table has key constraints available
     * 
     * @param hasKey true if table has key constraints, else false
     */
    public void setHasKey (boolean hasKey) {
        this.hasKey = hasKey;
    }
    
    /**
     * Return whether or not this table has key constraints available
     * 
     * @return true if table has key constraints, else false
     */
    @JsonProperty ("hasKey")
    public boolean hasKey () {
        return hasKey;
    }
}
