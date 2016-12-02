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

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.plog.domain.DomainRecord;
import com.dbvisit.replicate.plog.domain.HeaderRecord;
import com.dbvisit.replicate.plog.file.PlogFile;
import com.dbvisit.replicate.plog.format.EntryRecord;
import com.dbvisit.replicate.plog.format.EntryTagRecord;
import com.dbvisit.replicate.plog.format.EntryTagType;
import com.dbvisit.replicate.plog.format.EntryTagType.EntryFeature;
import com.dbvisit.replicate.plog.format.decoder.SimpleDataDecoder;

/**
 * Parses the control header record for a PLOG, it holds information
 * needed to decode PLOG entries and it's feature set
 */
public class HeaderParser implements DomainParser {
    private static final Logger logger = LoggerFactory.getLogger(
        HeaderParser.class
    );
    
    /** Parsed header record */
    private HeaderRecord hdr;
    
    /**
     * Parses a PLOG header record from raw control header PLOG entry record
     * 
     * @param plog Handle to PLOG that represent the parsed PLOG file on disk
     * @param rec  The raw control header PLOG entry record read from stream
     * 
     * @throws Exception if a parse error occurred
     */
    @Override
    public void parse(PlogFile plog, EntryRecord rec) throws Exception {
        if ((plog == null || !plog.canUse())) {
            throw new Exception ("FATAL: Invalid PLOG file provided to parse");
        }
        
        if (rec.isHeader()) {
            hdr = new HeaderRecord();
            parseFields(plog, rec);
            parseFeatures(plog, rec);
            hdr.setComplete(true);
        }
        else {
            hdr = null;
        }
    }
    
    /**
     * Parse the core fields of the control header
     * 
     * @param plog Handle to PLOG that represent the parsed PLOG file on disk
     * @param rec  The raw PLOG entry record read from byte stream
     * 
     * @throws Exception decoding a value for field from PLOG failed
     */
    private void parseFields (PlogFile plog, EntryRecord rec)
    throws Exception {
        /* default to incoming PLOG, will be overwritten if encoded */
        hdr.setPlogId(plog.getId());
        
        Map <EntryTagType, List<EntryTagRecord>> tags = rec.getEntryTags();
            
        EntryTagRecord tag;
        EntryTagType type = EntryTagType.TAG_PLOGSEQ;
        if (tags.containsKey(type)) {
            tag = tags.get (type).get(0);
            hdr.setPlogId (
                SimpleDataDecoder.decodeInteger (tag.getRawData())
            );
        }

        type = EntryTagType.TAG_LCR_ID;
        if (tags.containsKey(type)) {
            tag = tags.get (type).get(0);
            hdr.setId(
                SimpleDataDecoder.decodeLong(tag.getRawData()) 
                + (1000000000L * hdr.getPlogId())
            );
        }
        
        type = EntryTagType.TAG_MINE_UUID;
        if (tags.containsKey(type)) {
            tag = tags.get (type).get(0);
            hdr.setMineUUID(
                SimpleDataDecoder.decodeCharString(tag.getRawData())
            );
        }
    }
    
    /**
     * Parses the feature set supported by the PLOG file from the
     * raw control header record in PLOG
     *  
     * @param plog The PLOG that we are processing
     * @param rec  The raw control header PLOG entry record
     * 
     * @throws Exception when failing to decode the feature bit set
     */
    private void parseFeatures (PlogFile plog, EntryRecord rec)
    throws Exception {
        Map <EntryTagType, List<EntryTagRecord>> tags = rec.getEntryTags();
        
        /* check PLOG dictionary feature first */
        if (rec.isHeader() &&
            tags.containsKey(EntryTagType.TAG_FEATURES_1)) 
        {
            int features = SimpleDataDecoder.decodeInteger(
                tags.get (EntryTagType.TAG_FEATURES_1).get(0).getRawData()
            );
            
            boolean isCompact = (
                features & 
                EntryFeature.TAG_FEATURES_1_CACHE_DICT.getId()
            ) > 0;
            
            boolean hasJsonDict = (
                features & 
                EntryFeature.TAG_FEATURES_1_JSON_DICT.getId()
            ) > 0;
            
            boolean commitsOnly = (
                features & 
                EntryFeature.TAG_FEATURES_1_PESSIMISTIC_COMMIT.getId()
            ) > 0;
            
            boolean serialTx = (
                features & 
                EntryFeature.TAG_FEATURES_1_SERIALIZED_TXS.getId()
            ) > 0;   
            
            if (!hasJsonDict) {
                logger.error (
                    "PLOG file: " + plog.getFileName() + " do not have JSON " +
                    "meta data dictionary enabled, unable to parse"
                );
            }
            
            if (!commitsOnly) {
                logger.error (
                    "PLOG file: " + plog.getFileName() + " do not have " +
                    "pessimistic commit enabled, unable to parse"
                );
            }
                
            hdr.setCompactEncoding(isCompact);
            hdr.setJsonDictionary(hasJsonDict);
            hdr.setPessimisticCommit(hasJsonDict);
            hdr.setSerializedTransactions(serialTx);
        }
    }

    /**
     * Emit the complete, parsed PLOG header record to caller, internal
     * state is cleared when done
     * 
     * @return parsed and complete header domain record
     */
    @Override
    public DomainRecord emit() {
        HeaderRecord result = null;
        if (hdr != null) {
            result = hdr;
            hdr = null;
        }
        
        return result;
    }

    /**
     * Check if the current parsed header ready is complete and ready
     * to be emitted
     * 
     * @return true if complete and ready, else false
     */
    @Override
    public boolean canEmit() {
        boolean emit = false;
        if (hdr != null && hdr.isComplete()) {
            emit = true;
        }
        return emit;
    }

    /**
     * Header records cannot be multi-part, do not support merging
     * 
     * @return false do not support merging
     */
    @Override
    public boolean supportMultiPartMerging() {
        return false;
    }

    /**
     * Throws an exception if we ignored the fact that we do not support
     * merging
     * 
     * @throws Exception unsupported action
     */
    @Override
    public void enableMultiPartMerging() throws Exception {
        throw new UnsupportedOperationException(
            "Header data records are not multi-part records"
        );
    }

    /**
     * Header record is not an aggregate record
     * 
     * @return false not an aggregate parser
     */
    @Override
    public boolean isAggregateParser() {
        return false;
    }
    
}
