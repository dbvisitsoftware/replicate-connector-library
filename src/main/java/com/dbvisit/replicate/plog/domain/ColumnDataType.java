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

import java.util.HashMap;
import java.util.Map;

/**
 * Define all the column data types supported in PLOG
 */
public enum ColumnDataType {
    /** Integer, long or decimal number type */
    NUMBER ("NUMBER"),
    /** Variable length character strings */
    VARCHAR2 ("VARCHAR2"),
    /** Variable length character strings */
    VARCHAR ("VARCHAR"),
    /** Fixed length character strings */
    CHAR ("CHAR"),
    /** Variable length character strings in national character set */
    NVARCHAR2 ("NVARCHAR2"),
    /** Variable length character strings in national character set */
    NVARCHAR ("NVARCHAR"),
    /** Fixed length character strings in national character set */
    NCHAR ("NCHAR"),
    /** Variable length character text data */
    LONG ("LONG"),
    /** Variable length byte strings or character data that should not 
     *  be converted */
    RAW ("RAW"),
    /** Binary data that is not interpreted */
    LONG_RAW ("LONG RAW"),
    /** Date/time */
    DATE ("DATE"),
    /** Time stamp without time zone */
    TIMESTAMP ("TIMESTAMP"),
    /** Time stamp with time zone */
    TIMESTAMP_WITH_TIME_ZONE ("TIMESTAMP WITH TIME ZONE"),
    /** Time stamp with relative time zone */
    TIMESTAMP_WITH_LOCAL_TIME_ZONE ("TIMESTAMP WITH LOCAL TIME ZONE"),
    /** Interval between day and second */
    INTERVAL_DAY_TO_SECOND ("INTERVAL DAY TO SECOND"),
    /** Interval between year and month */
    INTERVAL_YEAR_TO_MONTH ("INTERVAL YEAR TO MONTH"),
    /** Large character data */
    CLOB ("CLOB"),
    /** Large unicode character data in national character set */
    NCLOB ("NCLOB"),
    /** UTF16 character data */
    CLOB_UTF16 ("CLOB_UTF16"),
    /** Unstructured binary data */
    BLOB ("BLOB"),
    /** Unsupported type */
    UNKNOWN ("UNKNOWN");
    
    /** Raw data type as string */
    private String type;
    
    /** Internal index for translating raw data types to enum entry */
    private static Map<String, ColumnDataType> index = 
        new HashMap<String, ColumnDataType>();
    
    /** 
     * Create column data type for a given raw Oracle type, as string
     * 
     * @param type raw Oracle type, as string
     */
    private ColumnDataType (String type) {
        this.type = type;
    }
    
    /**
     * Build index for enum to convert raw Oracle types to enum type
     */
    private static void buildIndex () {
        for (ColumnDataType dataType : ColumnDataType.values()) {
            index.put (dataType.getType(), dataType);
        }
    }
    
    /**
     * Return the raw Oracle data type, as string for a ColumnDataType
     * 
     * @return raw Oracle type, as string
     */
    public String getType() {
        return type;
    }
    
    /**
     * Use the type index to translate a raw Oracle data type, as string,
     * to an enum entry
     * 
     * @param type raw Oracle data type, as string
     * @return ColumnDataType
     * @throws Exception when no column data entry exist for raw data type
     */
    public ColumnDataType find (String type) throws Exception {
        if (index.size() == 0) {
            buildIndex ();
        }
        
        ColumnDataType dataType = null;
        
        if (index.containsKey (type)) {
            dataType = index.get (type);
        }
        else {
            throw new Exception (
                "No column data type found for type: " + type
            );
        }
        
        return dataType;
    }
    
}
