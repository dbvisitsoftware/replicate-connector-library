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

/** 
 * Define the different states for a Column (field) after DDL operation
 */
public enum ColumnState {
    /** Default state, is newly created table by DDL */
    CREATED,
    /** Existing table, existing column has not been changed by DDL */
    UNCHANGED,
    /** Existing table, new column has been added by DDL */
    ADDED,
    /** Existing table, existing column has been removed by DDL */
    REMOVED,
    /** Existing table, existing column has been modified by DLL */
    MODIFIED
}
