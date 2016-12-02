package com.dbvisit.replicate.plog.format;

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
 * Raw record for entry tag consisting of fixed 2 chunk section followed by
 * variable section of data chunks
 */
public class EntryTagRecord implements Comparable<EntryTagRecord> {
    /** The number of chunks before variable data chunks start */
    public static final int DATA_CHUNK_OFFSET = 2;
    /** Chunk length of tag record */
    private int length;
    /** Tag ID, corresponds to EntryTagType */
    private int id;
    /** Raw data chunks */
    private int[] rawData;
    /** 
     * Sequence of tag as encoded in parent record, not part of format 
     * but used for sorting 
     */
    private int sequence;

    /**
     * Set the length of complete entry tag record, as number of chunks in
     * PLOG, each a 4 byte size
     * 
     * @param length length of record as number of tags
     */
    public void setLength (int length) {
        this.length = length;
    }

    /**
     * Return the chunk length of tag record in PLOG
     * 
     * @return chunk length of complete tag record including pay load
     */
    public int getLength () {
        return this.length;
    }

    /**
     * Set the ID of the tag type as decoded from PLOG tag record in stream
     * 
     * @param id The ID value for tag, this is the type of tag
     */
    public void setId (int id) {
        this.id = id;
    }

    /**
     * Return the ID value for the tag which is the type ID of tag
     * 
     * @return the ID of tag type
     */
    public int getId () {
        return this.id;
    }

    /**
     * Set the raw chunk data as parsed from byte stream of PLOG
     * 
     * @param rawData the raw, un-decoded chunk data parsed for this PLOG
     *                tag record
     */
    public void setRawData (int [] rawData) {
        this.rawData = rawData;
    }

    /**
     * Return the raw chunk data for this tag record
     * 
     * @return chunk data, as integer array, containing data encoded in 
     *         raw PLOG or Oracle format
     */
    public int[] getRawData () {
        return this.rawData;
    }

    /**
     * Set the sequence of this tag in the parent entry record, this is the
     * implicit order as encoded in parent record in PLOG
     * 
     * @param sequence the sequence number of this tag in parent entry record
     */
    public void setSequence (int sequence) {
        this.sequence = sequence;
    }

    /**
     * Return sequence number of this tag in parent entry record, this is
     * its encoded order in the pay load of tags of the parent record
     * 
     * @return sequence number of this tag within parent pay load
     */
    public int getSequence () {
        return this.sequence;
    }

    /**
     * Compare two entry tag records only by its sequence in its parent
     * PLOG entry record
     * 
     * @return -1 if record is before other tag, 0 if at the same index else
     *         +1 if this record is encoded after the other tag 
     */
    @Override
    public int compareTo(EntryTagRecord o) {
        return this.sequence - o.sequence;
    }

    /** Clear out chunk data */
    public void clear () {
        rawData = null;
    }
}
