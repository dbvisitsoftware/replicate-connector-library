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
import com.dbvisit.replicate.plog.file.PlogFile;
import com.dbvisit.replicate.plog.format.EntryRecord;
import com.dbvisit.replicate.plog.format.EntryTagRecord;
import com.dbvisit.replicate.plog.format.EntryTagType;
import com.dbvisit.replicate.plog.format.EntryType;
import com.dbvisit.replicate.plog.format.decoder.SimpleDataDecoder;
import com.dbvisit.replicate.plog.reader.DomainReader;
import com.dbvisit.replicate.plog.reader.PlogStreamReader;

/** 
 * Act as proxy for parsing domain records from PLOG files included
 * in parent PLOG.
 */
public class ProxyDomainParser implements DomainParser {
    private static final Logger logger = LoggerFactory.getLogger(
        ProxyDomainParser.class
    );
        
    /** The PLOG file that this domain parser acts as a proxy for */
    private PlogFile childPlog;
    /** Sequence ID of included PLOG */
    private int childId;
    /** Name describing included PLOG */
    private String plogName;
    /** File name of included PLOG, not full path */
    private String plogFileName;
    /** The start SCN of included file record, when to start loading PLOG */
    private long startSCN;
    /** The end SCN of included statistics record, when to stop */
    private long endSCN;
    /** The row count of records in included PLOG, encoded in parent PLOG */
    private int rowCount;
    /** Counter of data records parsed in included PLOG, for validation */
    private int dataRecords = 0;
    /** Whether or not proxy is busy reading from included PLOG */
    private boolean busyReading = false;
    /** The proxy domain record to emit */
    private DomainRecord proxyRecord;
    /** Offset of end of parent record in parent PLOG, to ensure
     *  that replicate offset is contiguous for parent stream we always
     *  emit children records at parent's replicate offset
     */
    private long parentOffset = 0L;
    
    /**
     * Parses the parent PLOG for include file and stats entry records, it
     * becomes a proxy for parsing the data stream from the included 
     * PLOGs, effectively pausing the parsing of parent data stream 
     * until done. 
     * 
     * @param plog Parent PLOG file
     * @param rec  Parsed entry record where parent stream has been paused at
     * 
     * @throws Exception when parse error occurred
     */
    @Override
    public void parse(PlogFile plog, EntryRecord rec) throws Exception {
        if (busyReading) {
            /* busy reading child PLOG, proxy parse */
            parseChildRecord (plog);
            
            if (logger.isTraceEnabled() && canEmit()) {
                logger.trace (proxyRecord.toJSONString());
            }
        }
        else {
            /* parse parent PLOG */
            if (rec.isIncludeFileRecord()) {
                /* parse ESTYPE_LCR_PLOG_IFILE
                 * - open included PLOG stream
                 * - setup DomainReader to be same as parent
                 * - let domain reader parse and emit them here
                 */
                parseCoreFields (rec);
                openChildPlog (plog);
            }
            else if (rec.isIncludeFileStatsRecord()) {
                /* parse ESTYPE_LCR_PLOG_IFILE_STATS
                 * - validate number of rows parsed
                 * - close included PLOG stream
                 * - emit and close
                 */
                parseStatsFields (rec);
                
                /* verify data record count unless it's an aggregate reader or
                 * all records have been filtered
                 */
                if (dataRecords != 0 &&
                    dataRecords != rowCount && 
                    !plog.getReader().getDomainReader().isAggregateReader()) 
                {
                    logger.error (
                        "Parsed: " + dataRecords + " records from child PLOG: " +
                        plogFileName + ", expected: " + rowCount + " records"
                    );
                }
                else if (dataRecords == rowCount) {
                    logger.info (
                        "Parsed: " + dataRecords + " records from child " +
                        "PLOG: " + plogFileName
                    );
                }
                
                /* reset counter for parser */
                dataRecords = 0;
            }
        }
        
        if (busyReading) {
            /* force the parent PLOG reader to keep re-parsing the same
             * record until we're done emitting data in it's place
             */
            plog.getReader().rewind();
            plog.getReader().setPaused(true);
        }
        else {
            plog.getReader().setPaused(false);
        }
    }
    
    /**
     * Parse the fields that identifies the included PLOG
     * 
     * @param rec Parsed PLOG IFILE entry record
     * 
     * @throws Exception Failed to convert core LCR fields
     */
    private void parseCoreFields (EntryRecord rec) 
    throws Exception {
        try {
            Map <EntryTagType, List<EntryTagRecord>> tags = 
                rec.getEntryTags();
            
            EntryTagRecord tag;
            
            EntryTagType type = EntryTagType.TAG_PLOGSEQ;
            if (tags.containsKey(type)) {
                tag = tags.get (type).get(0);
                this.childId =
                    SimpleDataDecoder.decodeInteger (tag.getRawData());
            }
            else {
                throw new Exception (
                    "Invalid IFILE entry record, no PLOG sequence number"
                );
            }
            
            type = EntryTagType.TAG_PLOGNAME;
            if (tags.containsKey(type)) {
                tag = tags.get (type).get(0);
                this.plogName =
                    SimpleDataDecoder.decodeCharString(tag.getRawData());
            }
            else {
                throw new Exception (
                    "Invalid IFILE entry record, no PLOG name"
                );
            }
            
            type = EntryTagType.TAG_PLOG_FILENAME;
            if (tags.containsKey(type)) {
                tag = tags.get (type).get(0);
                this.plogFileName =
                    SimpleDataDecoder.decodeCharString(tag.getRawData());
            }
            else {
                throw new Exception (
                    "Invalid IFILE entry record, no PLOG file path name"
                );
            }
            
            type = EntryTagType.TAG_SCN;
            if (tags.containsKey(type)) {
                tag = tags.get (type).get(0);
                this.startSCN =
                    SimpleDataDecoder.decodeLong (tag.getRawData());
            }
            else {
                throw new Exception (
                    "Invalid IFILE entry record, no SCN"
                );
            }
        } catch (Exception e) {
            throw new Exception (
                "Failed to decode LCR properties, reason: " + e.getMessage(),
                e
            );
        }
    }
    
    /**
     * Parse the fields that provides stats for the included PLOG
     * 
     * @param rec Parsed PLOG IFILE STATS entry record
     * 
     * @throws Exception Failed to convert core LCR fields
     */
    private void parseStatsFields (EntryRecord rec) 
    throws Exception {
        try {
            Map <EntryTagType, List<EntryTagRecord>> tags = 
                rec.getEntryTags();
            
            EntryTagRecord tag;
            
            EntryTagType type = EntryTagType.TAG_PLOGSEQ;
            if (tags.containsKey(type)) {
                tag = tags.get (type).get(0);
                int id = SimpleDataDecoder.decodeInteger (tag.getRawData());
                
                if (id != childId) {
                    throw new Exception (
                        "Invalid IFILE STATS PLOG ID: " + id + ", reason: " +
                        "not the same as IFILE PLOG ID: " + childId
                    );
                }
            }
            else {
                throw new Exception (
                    "Invalid IFILE STATS entry record, no PLOG sequence number"
                );
            }
            
            type = EntryTagType.TAG_PLOGNAME;
            if (tags.containsKey(type)) {
                tag = tags.get (type).get(0);
                String name =
                    SimpleDataDecoder.decodeCharString(tag.getRawData());
                
                if (!name.equals (plogName)) {
                    throw new Exception (
                        "Invalid IFILE STATS PLOG name: " + name + ", " + 
                        "reason: not the same as IFILE PLOG name: "     + 
                        plogName
                    );  
                }
            }
            else {
                throw new Exception (
                    "Invalid IFILE STATS entry record, no PLOG name"
                );
            }
            
            type = EntryTagType.TAG_PLOG_FILENAME;
            if (tags.containsKey(type)) {
                tag = tags.get (type).get(0);
                String fileName =
                    SimpleDataDecoder.decodeCharString(tag.getRawData());
                
                if (!fileName.equals(plogFileName)) {
                    throw new Exception (
                        "Invalid IFILE STATS PLOG name: " + fileName + ", " +
                        "reason: not the same as IFILE PLOG file name: "    + 
                        plogFileName
                    ); 
                }
            }
            else {
                throw new Exception (
                    "Invalid IFILE STATS entry record, no PLOG file path name"
                );
            }
            
            type = EntryTagType.TAG_SCN;
            if (tags.containsKey(type)) {
                tag = tags.get (type).get(0);
                this.endSCN =
                    SimpleDataDecoder.decodeLong (tag.getRawData());
            }
            else {
                throw new Exception (
                    "Invalid IFILE  entry record, no SCN"
                );
            }
            
            type = EntryTagType.TAG_ROW_COUNT;
            if (tags.containsKey(type)) {
                tag = tags.get (type).get(0);
                this.rowCount =
                    SimpleDataDecoder.decodeInteger (tag.getRawData());
            }
            else {
                throw new Exception (
                    "Invalid IFILE STATS entry record, no PLOG row count"
                );
            }
            
            if (startSCN > endSCN) {
                throw new Exception (
                    "Invalid SCN for children PLOG, startSCN: " +
                    startSCN + " " + " endSCN: " + endSCN
                );
            }
        } catch (Exception e) {
            throw new Exception (
                "Failed to decode LCR properties, reason: " + e.getMessage(),
                e
            );
        }
    }
    
    /**
     * Opens the included child PLOG by inheriting domain reader properties
     * from it's parent PLOG data stream
     * 
     * @param parent Parent PLOG file that contains the reference to the
     *               child PLOG to process
     *               
     * @throws Exception when parse error occurs
     */
    private void openChildPlog (PlogFile parent) throws Exception {
        logger.info ("Opening child PLOG: " + childId + " " + plogName);
        
        childPlog = new PlogFile (
            parent.getId(),
            childId,
            parent.getTimestamp(),
            parent.getBaseDirectory(),
            plogFileName
        );
        
        childPlog.open();
        
        /* inherit domain reader behavior from parent */
        DomainReader parentReader = parent.getReader().getDomainReader();
        DomainReader childReader = childPlog.getReader().getDomainReader();
        
        Map<EntryType, DomainParser[]> copyOfParsers = 
            new HashMap<EntryType, DomainParser[]>();
            
        copyOfParsers.putAll(parentReader.getDomainParsers());
        childReader.setDomainParsers(copyOfParsers);
        /* remove IFILE parser, aka this parser */
        childReader.getDomainParsers().remove (EntryType.ETYPE_LCR_PLOG);
        childReader.setParseCriteria(parentReader.getParseCriteria());
        childReader.setFilterCriteria(parentReader.getFilterCriteria());
        childReader.setPersistCriteria(parentReader.getPersistCriteria());
        childReader.setIsAggregateReader(parentReader.isAggregateReader());
        childReader.enableFlushLastTransactions();
        
        /* get parent's cache */
        childPlog.copyCacheFrom(parent);
        
        /* start and emit all records in child streams at parent record's
         * end offset to ensure that all offset based filtering still 
         * works as if this was part of parent stream
         */
        parentOffset = parent.getReader().getOffset();
        childPlog.getReader().setOffset(parentOffset);
        
        /* flush every row to mimic single parse/emit at parent record's
         * replicate offset
         */
        childPlog.getReader().setFlushSize(1);
        
        busyReading = true;
    }
    
    /**
     * Proxy parse of child records in included PLOG
     * 
     * @param parent Parent PLOG for IFILE PLOG
     * 
     * @throws Exception when parse error occurs
     */
    private void parseChildRecord (PlogFile parent) throws Exception {
        PlogStreamReader reader = childPlog.getReader(); 
        
        if (reader.isDone()) {
            busyReading = false;
            
            logger.info ("Closing child PLOG: " + childId + " " + plogName);
            childPlog.close();
        }
        else {
            while (!reader.isDone() && !reader.canFlush()) {
                reader.read();
            }
            
            if (parent.getSchemas().size() != childPlog.getSchemas().size() ||
                !parent.getSchemas().equals(childPlog.getSchemas()))
            {
                if (logger.isTraceEnabled()) {
                    logger.trace (
                        "Copying child PLOG schema: "     + 
                        childPlog.getSchemas().toString() + 
                        " to parent"
                    );
                }
                    
                /* a new schema has been parsed from NOOP, add it */
                parent.copyCacheFrom(childPlog);
                /* set parent to having updated schemas */
                parent.setUpdatedSchema(true);
            }
            
            List<DomainRecord> flushed = reader.flush();
            
            if (flushed.size() > 0) {
                proxyRecord = flushed.get (0);
                
                if (proxyRecord.isDataRecord()) {
                    dataRecords++;
                }
            }
        }
    }
    
    /** 
     * Emit the proxy domain record as parsed from child PLOG 
     * 
     * @return DomainRecord the domain record parsed from child PLOG
     */
    @Override
    public DomainRecord emit() {
        DomainRecord result = proxyRecord;
        proxyRecord = null;
        
        return result;
    }

    /**
     * Test to see if the proxy domain record is ready to be emitted
     * 
     * @return true when domain record is ready, else false
     */
    @Override
    public boolean canEmit() {
        return busyReading && proxyRecord != null;
    }

    /**
     * The proxy parser do not have to support multi-part record merging,
     * it inherits all it's properties from the parent.
     * 
     * @return false, the proxy parser has no need to support merging
     */
    @Override
    public boolean supportMultiPartMerging() {
        /* proxy only, so not directly supported */
        return false;
    }

    /**
     * The proxy parser cannot merge multi-part records, it's behavior is
     * derived.
     */
    @Override
    public void enableMultiPartMerging() throws Exception {
        /* proxy only */
    }


    /**
     * The proxy parser cannot aggregate records, it's behavior is derived.
     * 
     * @return false, the proxy parser cannot be an aggregate reader
     */
    @Override
    public boolean isAggregateParser() {
        /* proxy only */
        return false;
    }

}
