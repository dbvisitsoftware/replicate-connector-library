package com.dbvisit.replicate.plog.domain.util;

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

import com.dbvisit.replicate.plog.domain.DomainRecord;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/** 
 * Simple utility class to setup JSON handling for domain records using 
 * Jackson object mapper.
 */
public class DomainJSONConverter {
    /** 
     * Configure the object mapper to exclude null or empty properties 
     * and to not fail on unknown properties when de-serializing
     */
    private static final ObjectMapper mapper = 
        new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
            .configure(
                DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, 
                false
            );
    
    /**
     * Helper function to convert a domain record to JSON
     * 
     * @param rec Domain record to convert
     * @return domain record serialized as JSON string
     * 
     * @throws Exception if any parse errors occur
     */
    public static String toJSONString(DomainRecord rec)
    throws Exception 
    {
        return mapper.writeValueAsString(rec);
    }
    
    /**
     * Helper function to de-serialize JSON to domain record object using
     * the value class definition provided
     * 
     * @param content   The JSON record, as string
     * @param valueType The value class that defines the structure
     * @param <T>       The domain record type parameter
     * 
     * @return Domain record of type T
     * 
     * @throws Exception if any de-serialization errors occur
     */
    public static <T> T fromJSONString (
        String content,
        Class<T> valueType
    ) throws Exception 
    {
        return mapper.readValue(content, valueType);
    }

}
