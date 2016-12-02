package com.dbvisit.replicate.plog.format.parser;

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

import java.io.DataInputStream;

import com.dbvisit.replicate.plog.format.EntryTagRecord;

/** 
 * Parses tag records for a PLOG entry from PLOG input stream
 */
public class EntryTagRecordParser extends Parser implements IParser {
    /** Use as singleton, it has no state */
    private EntryTagRecordParser () {}
    /** Use a single instance of the entry tag record parser */
    private static EntryTagRecordParser INSTANCE = new EntryTagRecordParser();

    /**
     * Return the instance of the tag record parser to use in parent parser
     * 
     * @return the instance of tag record parser to user
     */
    public static EntryTagRecordParser getParser() {
        return INSTANCE;
    }

    /**
     * Parse a PLOG entry tag record for parent entry record from byte
     * stream opened on a valid PLOG file. The format of a tag record
     * is as follows:
     * 
     * <pre>
     * chunk #1 - total length of tag record in number of chunks to decode
     * chunk #2 - unique ID of the tag that represents its type of pay load
     * chunk #3 - variable section of tag containing raw values in chunks
     * </pre>
     * 
     * @param input byte data input stream opened on PLOG
     * 
     * @return complete entry tag record that holds data pay load of change
     *         record
     * @throws Exception when any parse error occur on stream
     */
    public EntryTagRecord parse (final DataInputStream input) 
    throws Exception {
        EntryTagRecord tag = new EntryTagRecord();

        /* 2 chunks of fixed data */
        int length = parseSwappedInteger(input);
        int id = parseSwappedInteger(input);

        /* next variable section of chunks containing raw data in Oracle or
         * PLOG encoding format, to be decoded later
         */
        int[] rawData = new int[length - EntryTagRecord.DATA_CHUNK_OFFSET];

        for (int i = 0; i < length - EntryTagRecord.DATA_CHUNK_OFFSET; i++) {
            rawData[i] = parseSwappedInteger(input);
        }

        tag.setLength (length);
        tag.setId (id);
        tag.setRawData (rawData);

        return tag;
    }

}
