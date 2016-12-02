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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.dbvisit.replicate.plog.format.EntryRecord;
import com.dbvisit.replicate.plog.format.EntrySubType;
import com.dbvisit.replicate.plog.format.EntryTagRecord;
import com.dbvisit.replicate.plog.format.EntryTagType;

/** 
 * Parses PLOG entry records from PLOG input stream
 */
public class EntryRecordParser extends Parser implements IParser {
    /** Use as singleton, it has no state */
    private EntryRecordParser () {}
    /** Use a single instance of the entry record parser */
    private static EntryRecordParser INSTANCE = new EntryRecordParser();

    /**
     * Return the instance of entry record parser to use everywhere
     * 
     * @return instance of entry record parser to use
     */
    public static EntryRecordParser getParser() {
        return INSTANCE;
    }

    /**
     * Parses a PLOG entry record from data input stream. The format of entry
     * record are as follows:
     * 
     * <pre>
     * chunk #1 - total size of complete entry record as number of chunks
     * chunk #2 - main type of record
     * chunk #3 - sub type of record
     * chunk #4 - variable section of tag data
     * </pre>
     * 
     * @param input byte data input stream opened on PLOG
     * 
     * @return parsed PLOG entry record, complete with tag data pay load
     * @throws Exception when any parse error occur
     */
    public EntryRecord parse(final DataInputStream input) 
    throws Exception 
    {
        EntryRecord entry = new EntryRecord();

        /* decode fixed section of 3 chunks */
        int length  = parseSwappedInteger(input);
        int type    = parseSwappedInteger(input);
        int subtype = parseSwappedInteger(input);

        Map<EntryTagType, List<EntryTagRecord>> entryTags = 
            new HashMap<EntryTagType, List<EntryTagRecord>>();

        /* use tag record parser instance */
        IParser parser = EntryTagRecordParser.getParser();

        /* next parse the variable section of raw tags */
        int chunksRead = EntryRecord.DATA_CHUNK_OFFSET;
        int sequence = 0;
        while (chunksRead < length) {
            /* parse the tag record containing the raw data to decode */
            EntryTagRecord tag = (EntryTagRecord)parser.parse(input);
            tag.setSequence(sequence++);

            EntryTagType tagType = EntryTagType.TAG_UNKNOWN.find (tag.getId());
            if (entryTags.containsKey(tagType)) {
                entryTags.get(tagType).add(tag);
            }
            else {
                List<EntryTagRecord> tags = new LinkedList<EntryTagRecord>();
                tags.add (tag);
                entryTags.put (tagType, tags);
            }

            chunksRead += tag.getLength();
        }
        entry.setLength (length);
        entry.setTypeId (type);
        entry.setSubTypeId (subtype);
        entry.setEntryTags(entryTags);
        entry.setSubType(EntrySubType.ESTYPE_UNKNOWN.find (type, subtype));

        return entry;
    }

}
