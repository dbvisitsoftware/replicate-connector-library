package com.dbvisit.replicate.plog.domain.parser;

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
import com.dbvisit.replicate.plog.file.PlogFile;
import com.dbvisit.replicate.plog.format.EntryRecord;

/**
 * Define behavior for parsing PLOG entries to domain objects 
 */
public interface DomainParser {
    /** 
     * All parsers must parse a PLOG entry record read from stream
     * using and maintaining cache in PLOG file
     * 
     * @param plog Handle to PLOG that represent the parsed PLOG file on disk
     * @param rec  The raw PLOG entry record read from byte stream
     * 
     * @throws Exception for any parse errors
     */
    void parse (PlogFile plog, EntryRecord rec) throws Exception;
    
    /**
     * Emit the complete DomainRecord to return to caller
     * 
     * @return parsed, complete domain record
     */
    DomainRecord emit();
    
    /**
     * Check if the parser has a complete and valid parsed domain record
     * to emit to caller
     * 
     * @return true if domain record is ready to be emitted, else false
     */
    boolean canEmit();
    
    /**
     * Check if the parser supports merging multi-part records, the meaning
     * thereof depends on the beavior of the domain parser
     * 
     * @return true if domain parser supports merging of what constitutes
     *         a multi-part record
     */
    boolean supportMultiPartMerging();
    
    /**
     * Instruct the domain parser to enable the merging of multi-part records,
     * each parser will have a different interpretation what a multi-part
     * record constitutes
     * 
     * @throws Exception when it's impossible to merge
     */
    void enableMultiPartMerging() throws Exception;
    
    /**
     * Check whether or not the domain parser takes care of parsing
     * single records, but aggregating a different view of them during
     * parsing, this changes behavior in how parse criteria is applied
     * 
     * @return true if the domain parser is aggregating PLOG information to
     *         create an aggregate record, else false
     */
    boolean isAggregateParser();
}
