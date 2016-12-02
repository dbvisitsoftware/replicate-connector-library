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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.plog.domain.DomainRecord;
import com.dbvisit.replicate.plog.domain.MetaDataRecord;
import com.dbvisit.replicate.plog.file.PlogFile;
import com.dbvisit.replicate.plog.format.EntryRecord;
import com.dbvisit.replicate.plog.format.EntryTagRecord;
import com.dbvisit.replicate.plog.format.EntryTagType;
import com.dbvisit.replicate.plog.format.decoder.SimpleDataDecoder;
import com.dbvisit.replicate.plog.metadata.DDLMetaData;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MetaDataParser implements DomainParser {
    /** Ignore all virtual columns encoded at ordinal number 0 */
    private final int VIRTUAL_COLUMN_ID = 0;
    
    private static final Logger logger = LoggerFactory.getLogger(
        MetaDataParser.class
    );

    /** Parsed meta data record */
    private MetaDataRecord mdr;
    
    /**
     * Parses PLOG schema cache from a PLOG entry record that has dictionary
     * data encoded as JSON document
     * 
     * @param plog Current PLOG to hold meta data cache
     * @param rec  PLOG entry record that has dictionary encoded in JSON
     *             as pay load
     *             
     * @throws Exception if a meta data parse error occur, including
     *                   JSON de-serialization error
     */
    @Override
    public void parse(PlogFile plog, EntryRecord rec) throws Exception {
        if ((plog == null || !plog.canUse())) {
            throw new Exception ("FATAL: Invalid PLOG file provided to parse");
        }
        
        if (rec.isJSONMetaData()) {
            mdr = new MetaDataRecord();
        
            parseFields (plog, rec);
            parseMetaDataJSON (plog, rec);
        }
        else {
            mdr = null;
        }
       
    }
    
    /**
     * Parses the core fields for a meta data domain record
     * 
     * @param plog The current PLOG to parse and cache meta data for
     * @param rec  The entry record with meta data in JSON format
     * 
     * @throws Exception if any parse errors occur
     */
    private void parseFields (PlogFile plog, EntryRecord rec)
    throws Exception {
        /* default to incoming PLOG, will be overwritten if encoded */
        mdr.setPlogId(plog.getId());
        
        Map <EntryTagType, List<EntryTagRecord>> tags = rec.getEntryTags();
            
        EntryTagRecord tag;
        EntryTagType type = EntryTagType.TAG_PLOGSEQ;
        if (tags.containsKey(type)) {
            tag = tags.get (type).get(0);
            mdr.setPlogId (
                SimpleDataDecoder.decodeInteger (tag.getRawData())
            );
        }

        type = EntryTagType.TAG_LCR_ID;
        if (tags.containsKey(type)) {
            tag = tags.get (type).get(0);
            mdr.setId(
                SimpleDataDecoder.decodeLong(tag.getRawData()) 
                + (1000000000L * mdr.getPlogId())
            );
        }

        type = EntryTagType.TAG_SCN;
        if (tags.containsKey(type)) {
            tag = tags.get (type).get(0);
            mdr.setSCN(
                SimpleDataDecoder.decodeLong (tag.getRawData())
            );
        }
    }
    
    /**
     * Parse the meta data field of raw PLOG entry by de-serializing the JSON 
     * pay load and caching it in PLOG
     * 
     * @param plog The current PLOG to cache the parsed meta data
     * @param rec  The PLOG entry record with dictionary as JSON
     * 
     * @throws Exception if any parse error occurs
     */
    private void parseMetaDataJSON (PlogFile plog, EntryRecord rec) 
        throws Exception 
    {
        if (mdr == null) {
            throw new Exception ("FATAL: Meta data record is NULL");
        }
        
        String json = null;
        
        EntryTagType type = EntryTagType.TAG_JSON_TEXT;
        Map <EntryTagType, List<EntryTagRecord>> tags = rec.getEntryTags();
        
        if (tags.containsKey(type)) {
            EntryTagRecord tag = tags.get (type).get(0);
            json = 
                SimpleDataDecoder.decodeCharString(tag.getRawData());
        }
        
        if (json != null && json.length() >= 0) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                DDLMetaData ddl = 
                    mapper.readValue (json, DDLMetaData.class);
                
                if (!ddl.isValid()) {
                    throw new Exception (
                        "Failed reading DDL from JSON: " + json       
                    );
                }
                
                /* filter virtual columns from index */
                if (ddl.getColumns().containsKey (VIRTUAL_COLUMN_ID)) {
                    ddl.getColumns().remove (VIRTUAL_COLUMN_ID);
                }
                /* filter virtual columns */
                int i = 0;
                while (ddl.getTableColumns().get(i).getId() == VIRTUAL_COLUMN_ID)
                {
                    ddl.getTableColumns().remove(i);
                }
                
                String schemaName = ddl.getSchemataName();

                if (plog.getSchemas() == null) {
                    plog.setSchemas(new HashMap<String, DDLMetaData>());
                }
                
                Map<String, DDLMetaData> schemas = plog.getSchemas();

                if (schemas.containsKey (schemaName)) {
                    /* check for update */
                    DDLMetaData md = schemas.get (schemaName);
                    
                    if (ddl.getValidSinceSCN() > md.getValidSinceSCN()) {
                        plog.setUpdatedSchema(true);
                        plog.getSchemas().put(schemaName, ddl);
                        mdr.setComplete(true);
                    }
                }
                else {
                    plog.getSchemas().put(schemaName, ddl);
                    plog.setUpdatedSchema(true);
                    mdr.setComplete(true);
                }
                
                mdr.setMetaData(ddl);
            }
            catch (Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug ("Failed to parse JSON meta data: " + json);
                    logger.debug ("Cause: ", e);
                }
                throw new Exception (
                    "Failed to parse JSON meta data, reason: " +
                    e.getMessage()
                );
            }
        }
    }

    /** 
     * Flush the current meta data record and return to caller, reset
     * 
     * @return meta data domain record
     */
    @Override
    public DomainRecord emit() {
        MetaDataRecord result = null;
        if (mdr != null) {
            result = mdr;
            mdr = null;
        }
        
        return result;
    }

    /**
     * Check if meta data record is complete and ready to be used
     * 
     * @return true if record is ready and can be emitted, else false
     */
    @Override
    public boolean canEmit() {
        boolean emit = false;
        if (mdr != null && mdr.isComplete()) {
            emit = true;
        }
        return emit;
    }

    /**
     * Meta data records cannot be multi-part, do not suppport merging
     * 
     * @return false, do not support multi-part merging
     */
    @Override
    public boolean supportMultiPartMerging() {
        return false;
    }

    /**
     * Meta data records cannot be multi-part, throw an unsupported
     * operation exception
     * 
     * @throws Exception unsupported action
     */
    @Override
    public void enableMultiPartMerging() throws Exception {
        throw new UnsupportedOperationException(
            "DDL Meta data JSON records are not multi-part records"
        );
    }

    /**
     * Meta data is not aggregate records, return false
     * 
     * @return false, not an aggregate parser
     */
    @Override
    public boolean isAggregateParser() {
        return false;
    }
}
