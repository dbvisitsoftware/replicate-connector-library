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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Meta data for table dictionary as decoded from row-level changes in PLOG
 */
public class Table {
    /** The ID for table object in Oracle dictionary */
    @JsonProperty("tableId")
    private Integer id;
    /** The owner, schema, for this object in Oracle */
    @JsonProperty("tableOwner")
    private String owner;
    /** The name of table, as defined in Oracle dictionary */ 
    @JsonProperty("tableName")
    private String name;
    /** The list of incomplete column data decoded from a row level change */
    @JsonProperty("tableColumns")
    private List<Column> columns;

    /**
     * Set the object ID of table in Oracle
     * 
     * @param tableId the internal Oracle ID for table object
     */
    public void setId (Integer tableId) {
        this.id = tableId;
    }

    /**
     * Return the object ID of table in Oracle dictionary
     * 
     * @return ID of table in internal Oracle dictionary
     */
    public Integer getId () {
        return this.id;
    }

    /**
     * Set the user schema object that owns this table 
     * 
     * @param tableOwner name of owner of table in dictionary
     */
    public void setOwner (String tableOwner) {
        this.owner = tableOwner;
    }

    /**
     * Return the name of the user schema object that owns this table
     *  
     * @return raw name of user schema, owner in Oracle, may include <em>$</em>
     */
    public String getOwner () {
        return this.owner;
    }

    /**
     * Set the name of the table object in Oracle
     * 
     * @param tableName name of table in Oracle
     */
    public void setName (String tableName) {
        this.name = tableName;
    }

    /**
     * Return the name of table in Oracle
     * 
     * @return raw name of user table in Oracle, may include <em>$</em>
     */
    public String getName () {
        return this.name;
    }

    /**
     * Set the list of columns decoded from column meta data in row-level
     * change in PLOG, including ordinal number, name and type of column
     * 
     * @param columns column meta data for columns present in table
     */
    public void setColumns (List <Column> columns) {
        this.columns = columns;
    }

    /**
     * Return the list of columns defined in row-level change as dictionary
     * information for decoding row data in compactly encoded PLOG
     * 
     * @return column meta data for columns present in table row
     */
    public List <Column> getColumns () {
        return this.columns;
    }

    /**
     * Return string representation of table meta data for logging
     * 
     * @return table meta data as string
     */
    public String toString() {
        return "Table id: " + id    + " " +
               "owner: "    + owner + " " +
               "name:"      + name  + " " +
               "columns: "  + columns.toString();
    }

}
