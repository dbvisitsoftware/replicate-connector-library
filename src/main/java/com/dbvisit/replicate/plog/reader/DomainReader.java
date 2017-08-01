package com.dbvisit.replicate.plog.reader;

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

import java.io.EOFException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.plog.domain.DomainRecord;
import com.dbvisit.replicate.plog.domain.ReplicateOffset;
import com.dbvisit.replicate.plog.domain.TransactionInfoRecord;
import com.dbvisit.replicate.plog.domain.parser.DomainParser;
import com.dbvisit.replicate.plog.file.PlogFile;
import com.dbvisit.replicate.plog.format.EntryRecord;
import com.dbvisit.replicate.plog.format.EntrySubType;
import com.dbvisit.replicate.plog.format.EntryType;
import com.dbvisit.replicate.plog.format.decoder.SimpleDataDecoder;
import com.dbvisit.replicate.plog.format.EntryTagRecord;
import com.dbvisit.replicate.plog.format.EntryTagType;
import com.dbvisit.replicate.plog.format.parser.EntryRecordParser;
import com.dbvisit.replicate.plog.format.parser.IFormatParser;
import com.dbvisit.replicate.plog.format.parser.FormatParser.StreamClosedException;
import com.dbvisit.replicate.plog.reader.criteria.Criteria;
import com.dbvisit.replicate.plog.reader.criteria.InternalDDLFilterCriteria;

/**
 * Read raw data in PLOG and convert to domain objects
 */
public final class DomainReader {
    /** Default wait time for arrival of new data at EOF of open PLOG stream */
    private static final int WAIT_TIME_MS = 1000;
    /** Define the interval of waits between reporting a DEBUG message */
    private static final int LOG_WAIT_INTERVAL = 1000;
    /** Define the time out waiting for data when replication was restarted */
    private static final int RESTART_WAIT_INTERVAL_TIME_OUT = 60;

    private static final Logger logger = LoggerFactory.getLogger(
        DomainReader.class
    );

    /** Post filter criteria for filtering PLOG entries or domain records */
    @SuppressWarnings("rawtypes")
    private final Criteria filterCriteria;
    /** Parse criteria for converting PLOG entries to domain records */
    @SuppressWarnings("rawtypes")
    private final Criteria parseCriteria;
    /** Persist type criteria for persisting domain records parsed from PLOG */
    @SuppressWarnings("rawtypes")
    private final Criteria persistCriteria;
    /** Domain record parsers lookup per PLOG entry record type */
    private final Map <EntryType, DomainParser[]> domainParsers;
    /** Raw entry data parser */
    private final IFormatParser parser;
    /** Default filter, always used */
    @SuppressWarnings("rawtypes")
    private final Criteria defaultCriteria;
    /** Domain reader is dedicated to reading aggregates */
    private final boolean aggregateReader;
    /** Emit any PLOG transaction data at end of file */
    private final boolean flushLastTransactions;
    /** Local cache of cached schema names */
    private final Map<Integer, String> schemaCache;

    /** Counter for recording how many times we've been waiting for new data */
    private int waiting;

    /**
     * Build a correctly configured immutable <em>DomainReader</em>
     */
    @SuppressWarnings("rawtypes")
    public static class DomainReaderBuilder {
        private Criteria filterCriteria;
        private Criteria parseCriteria;
        private Criteria persistCriteria;
        private Criteria defaultCriteria;
        private Map <EntryType, DomainParser[]> domainParsers;
        private boolean aggregateReader;
        private boolean flushLastTransactions;
        private boolean mergeMultiPartRecords;
        
        public DomainReaderBuilder() {}
        
        /**
         * Set the criteria that determines whether or not to filter a PLOG entry
         * and/or domain record after parsing
         * 
         * @param criteria Any criteria that can be used as filter criteria
         * @return this builder
         */
        public DomainReaderBuilder filterCriteria(final Criteria criteria) {
            filterCriteria = criteria;
            return this;
        }
        
        /** 
         * Set the criteria that determines which raw PLOG records are parsed 
         * and possible emitted as domain records
         *  
         * @param criteria Any criteria that can be used as parse criteria
         * @return this builder
         */
        public DomainReaderBuilder parseCriteria(final Criteria criteria) {
            parseCriteria = criteria;
            return this;
        }
        
        /** 
         * Set the criteria that determines which domain records parsed from 
         * PLOG should be persisted to domain cache in caller
         * 
         * @param criteria Any criteria that can be used as persist criteria
         * @return this builder                      
         */
        public DomainReaderBuilder persistCriteria(final Criteria criteria) {
            persistCriteria = criteria;
            return this;
        }
        
        /**
         * Set the criteria to be used as default filter, usually this is
         * set to filter internal dbvrep schema objects and DDL, but could
         * be anything. Note this criteria is always used by default
         * 
         * @param criteria default criteria to apply to all LCRs
         * @return this builder
         */
        public DomainReaderBuilder defaultCriteria(final Criteria criteria) {
            defaultCriteria = criteria;
            return this;
        }
        
        /**
         * Set the domain parsers to use by the domain reader for parsing and
         * converting raw PLOG format records to domain records to emit, 
         * grouped by type of PLOG entry record, each type may be parsed by 
         * multiple domain parsers
         * 
         * @param domainParsers the domain parsers to use, grouped by type of 
         *                      PLOG
         * @return this builder 
         */
        public DomainReaderBuilder domainParsers(
            final Map <EntryType, DomainParser[]> domainParsers
        ) {
            this.domainParsers = domainParsers;
            
            return this;
        }
        
        /**
         * Set whether or not this reader will be reading aggregate records, 
         * this allows adjusting the parse behavior of the aggregate domain
         * parser (only)
         * 
         * @param aggregateReader true if it has an aggregate domain parser 
         *                        registered, else false
         * @return this builder
         */
        public DomainReaderBuilder aggregateReader(
            final boolean aggregateReader
        ) {
            this.aggregateReader = aggregateReader;
            return this;
        }
        
        /**
         * Set whether or not the domain reader should flush the last 
         * transactions at PLOG boundary, these may not be complete, 
         * but it's up to the caller to decide how to handle these
         * 
         * @param flushLastTransactions true or false
         * @return this builder
         */
        public DomainReaderBuilder flushLastTransactions (
            final boolean flushLastTransactions
        ) {
            this.flushLastTransactions = flushLastTransactions;
            return this;
        }
        
        /**
         * Set whether or not the domain parsers used by domain reader being
         * built will enable the merging of multi-part records, each parser
         * will have its own interpretation of what that means
         * 
         * @param mergeMultiPartRecords true or false
         * @return this builder
         */
        public DomainReaderBuilder mergeMultiPartRecords (
            final boolean mergeMultiPartRecords
        ){ 
            this.mergeMultiPartRecords = mergeMultiPartRecords;
            return this;
        }
        
        private void validate() throws Exception {
            /* validate requirements for valid domain reader */
            if (domainParsers == null ||domainParsers.size() == 0) {
                throw new Exception (
                    "Unable to build a domain reader without domain " +
                    "parsers"
                );
            }
            
            if (defaultCriteria == null && aggregateReader) {
                /* default to filter all internal DDL for aggregate readers */
                defaultCriteria = 
                    new InternalDDLFilterCriteria<EntrySubType>();
            }
        }
        
        /**
         * Iterates through the registered domain parser and set them into their
         * merge mode that allows merging multi-part records into one complete
         * view, only if the individual parsers support it
         * 
         * @throws Exception if any of the parsers did not support merging, but
         *                   tried to set their mode to merge
         */
        private void enableMultiPartMerging () throws Exception {
            if (domainParsers != null) {
                for (DomainParser[] dps : domainParsers.values()) {
                    for (DomainParser dp : dps) {
                        if (dp.supportMultiPartMerging()) {
                            dp.enableMultiPartMerging();
                        }
                    }
                }
            }
        }
        
        public DomainReader build() throws Exception {
            validate();
            
            if (mergeMultiPartRecords) {
                enableMultiPartMerging();
            }
            
            return new DomainReader(
                filterCriteria,
                parseCriteria,
                persistCriteria,
                defaultCriteria,
                domainParsers,
                aggregateReader,
                flushLastTransactions
            );
        }
    }
    
    public static DomainReaderBuilder builder() {
        return new DomainReaderBuilder();
    }
    
    /**
     * Create and initialize a clean domain reader
     */
    @SuppressWarnings("rawtypes")
    private DomainReader (
        final Criteria filterCriteria,
        final Criteria parseCriteria,
        final Criteria persistCriteria,
        final Criteria defaultCriteria,
        final Map <EntryType, DomainParser[]> domainParsers,
        final boolean aggregateReader,
        final boolean flushLastTransactions
    ) {
        this.filterCriteria        = filterCriteria;
        this.parseCriteria         = parseCriteria;
        this.persistCriteria       = persistCriteria;
        this.defaultCriteria       = defaultCriteria;
        this.domainParsers         = domainParsers;
        this.aggregateReader       = aggregateReader;
        this.flushLastTransactions = flushLastTransactions;
        
        /* internal state, not done by builder */
        parser      = EntryRecordParser.getParser();
        schemaCache = new HashMap<Integer, String>();
        waiting     = 0;

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }
    
    /**
     * Return the persist criteria used to determine which records to persist
     * to domain cache for flushing later
     * 
     * @return persist criteria, usually by type or offset of record
     */
    @SuppressWarnings("rawtypes")
    public Criteria getPersistCriteria () {
        return this.persistCriteria;
    }

    /**
     * Return the criteria that determines which parsed records to filter
     * after done parsing, these records may be required for aggregation
     * information at parse time, but not afterwards
     * 
     * @return the criteria to filter records after parsing as domain
     */
    @SuppressWarnings("rawtypes")
    public Criteria getFilterCriteria () {
        return this.filterCriteria;
    }

    /**
     * Return the criteria that determines whether or not a PLOG entry record
     * needs to be parsed and converted to domain objects
     * 
     * @return the parse criteria
     */
    @SuppressWarnings("rawtypes")
    public Criteria getParseCriteria () {
        return this.parseCriteria;
    }
    
    /**
     * Return the domain parsers that will be used by this domain reader
     * for parsing, this is used when inheriting parsers from a parent
     * parser
     * 
     * @return groups of domain parser to use to read, convert and process
     *         raw PLOG entry records, each group per type of raw record
     */
    public Map<EntryType, DomainParser[]> getDomainParsers() {
        return this.domainParsers;
    }

    /**
     * Reads one PLOG entry record from input PLOG byte stream and parse
     * it with the registered domain records, returning a collection
     * of all domain records parsed by parsers from PLOG entry record
     * read
     * 
     * @param reader the PLOG stream handle for reading bytes
     * 
     * @return list of domain records
     * @throws Exception when any stream read, parse or domain conversion
     *                   issue occur
     */
    @SuppressWarnings("unchecked")
    public List<DomainRecord> read (final PlogStreamReader reader)
    throws Exception {
        List<DomainRecord> domainRecords = null;

        if (domainParsers == null || domainParsers.isEmpty()) {
            throw new Exception (
                "FATAL: no domain parsers registered, unable to read " +
                "PLOG data stream"
            );
        }

        reader.mark();

        PlogFile plog = reader.getPlog();

        EntryRecord rec = null;
        try {
            /* parse raw entry record */
            rec = (EntryRecord)parser.parse(reader.getPlogStream());
            
            /* successfully parsed a record, reset waiting */
            waiting = 0;

            /* parse SCN from tags before record owner */
            parseEntryRecordSCN(plog, rec);

            /* parse record owner from tags or PLOG dictionary */
            parseEntryRecordOwner(plog, rec);

            /* maintain offsets before parsing LCR */
            reader.advanceOffset(rec.getSize());

            /* store offset of the end of this record in its parent PLOG.
             * Once committed to output this is where the next record will
             * start and where the output reader should continue reading.  
             */
            ReplicateOffset rs = new ReplicateOffset (
                plog.getUID(), 
                reader.getOffset()
            );

            rec.setUniqueOffset(rs);

            if (logger.isTraceEnabled() && rec.isDataRecord()) {
                logger.trace (
                    "Schema: "   + rec.getRecordSchema() + " " +
                    "Type: "     + rec.getSubType()      + " " + 
                    "Offset: "   + rec.getRecordOffset() + " " +
                    "Size: "     + rec.getSize() +
                    (
                        parseCriteria != null 
                        ? " Parse: " + parseCriteria.meetCriteria(rec) 
                        : ""
                    )
                );
            }

            boolean hasNext = rec.isFooter() ? false : true;
                   
            /* check if we're done before applying filter */
            if (hasNext) {
                EntryType entryType = rec.getSubType().getParent();
                
                /* if there are no domain parsers configured for this record 
                 * type it is skipped
                 */
                if (domainParsers != null && 
                    domainParsers.containsKey (entryType)) {

                    for (DomainParser domainParser : 
                         domainParsers.get(entryType)) 
                    {
                        boolean parse = true;

                        if (defaultCriteria != null) {
                            /* apply default criteria to filter internal
                             * meta data and system tables only, parse all 
                             * other schema records
                             */
                            parse = defaultCriteria.meetCriteria(rec);
                        }
                        
                        if (parse &&
                            !domainParser.isAggregateParser() && 
                            parseCriteria != null)
                        {
                            /* apply parse criteria */
                            parse = parseCriteria.meetCriteria(rec);
                        }
                        
                        if (parse) {
                            DomainRecord domainRecord = null;

                            domainParser.parse (plog, rec);

                            if (domainParser.canEmit()) {
                                if (domainRecords == null) {
                                    domainRecords = 
                                        new LinkedList<DomainRecord>();
                                }
                                domainRecord = domainParser.emit();

                                /* the end of the transaction/record */
                                domainRecord.setReplicateOffset(rs);

                                /* for domain records that are aggregates we may
                                 * have to parse it's constituent parts first,
                                 * but filter it when we're done
                                 */
                                if (filterCriteria == null ||
                                    filterCriteria.meetCriteria(domainRecord))
                                {
                                    domainRecords.add (domainRecord);
                                }
                            }

                            if (domainRecord != null) {
                                if (domainRecord.isTransactionInfoRecord()) {
                                    TransactionInfoRecord txr =
                                        (TransactionInfoRecord) domainRecord;

                                    domainRecord.setPersist(txr.isValid());
                                }
                                else {
                                    domainRecord.setRawRecordSize(rec.getSize());
                                    if (persistCriteria != null) {
                                        domainRecord.setPersist(
                                            persistCriteria.meetCriteria(rec)
                                        );
                                    }
                                    else {
                                        /* default to persisting all domain 
                                         * records parsed */
                                        domainRecord.setPersist(true);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            else {
                if (flushLastTransactions) {
                    if (domainRecords == null) {
                        domainRecords = new LinkedList <DomainRecord>();
                    }

                    /* if forced it means caller is happy to flush possible
                     * partial transaction info records
                     */
                    for (TransactionInfoRecord txr : 
                        plog.getTransactionRecords().values())
                    {
                        txr.setReplicateOffset(rs);

                        if (txr.isValid()) {
                            txr.setPersist(true);
                            txr.setComplete(true);

                            domainRecords.add (txr);
                        }
                    }
                    plog.clearTransactionRecords();
                }

                /* no more entry records, reader is done and can 
                 * be released */
                reader.setDone (true);
            }
        } catch (EOFException eof) {
            if (logger.isDebugEnabled() && 
                waiting % LOG_WAIT_INTERVAL == 0) 
            {
                logger.debug (
                    "Waiting for new data or end of data in PLOG: " + 
                    plog.getFileName() + " at offset: " + 
                    reader.getOffset()
                );
            }

            /* PLOG entry is not complete yet, still streaming, so wait */
            reader.rewind();
            /* count number of times waiting for new data to arrive */
            waiting++;
            
            try {
                Thread.sleep (WAIT_TIME_MS);
            }
            catch (InterruptedException ie) {
                /* reader is done, it has been interrupted waiting for data */
                reader.setDone (true);
            }
            
            /* workaround for when a replication restart failed to finalize
             * the PLOG, as in write the ending PLOG footer, wait until
             * time out which is defined in wait intervals
             */
            if (plog.forceCloseAtEnd() && 
                waiting == RESTART_WAIT_INTERVAL_TIME_OUT) 
            {
                logger.warn (
                    "Incomplete PLOG: " + plog.getFileName() + " " +
                    "found when replication was restarted, closing"
                );
                
                reader.setDone (true);
            }
        } catch (StreamClosedException se) {
            throw se;
        } catch (Exception e) {
            /* catch errors and add some context, handle null error message */
            if (e.getMessage() == null) {
                /* no error message, add error for cause with stack trace,
                 * this is most likely due to internal and unexpected
                 * error
                 */
                logger.error ("An internal processing error has occurred", e);
            }
            
            String errMsg = (
                e.getMessage() != null
                ? e.getMessage() 
                :"an internal processing error has occurred"
            );
            
            throw new Exception (
                "Domain reader failed to read record " + 
                (
                    rec.getRecordSchema() != null 
                    ? "for: " + rec.getRecordSchema() + " "
                    : ""
                ) +
                "with type: " + rec.getRecordType() + " "  +
                "at SCN: "    + rec.getRecordSCN()  + ", " +
                "reason: " + errMsg,
                e
            );
        }
        finally {
            if (rec != null) {
              rec.clear();
            }
        }

        return domainRecords;
    }

    /**
     * Return whether or not this reader will be reading records with an
     * aggregate domain parser, this changes behavior for proxy domain
     * parser that inherits this domain reader's aggregate parser
     * 
     * @return true if it will read aggregate records, else false
     */
    public boolean isAggregateReader () {
        return this.aggregateReader;
    }
    
    /**
     * Return whether or not this reader will flush records for transactions
     * that are not complete yet at the end of PLOG stream
     * 
     * @return true or false
     */
    public boolean shouldFlushLastTransactions () {
        return this.flushLastTransactions;
    }

    /** 
     * Helper function to populate the full owner name of a raw
     * PLOG record to support <i>FilterableRecord</i> interface and criteria
     * 
     * @param plog PLOG file meta data
     */
    private void parseEntryRecordOwner (PlogFile plog, EntryRecord rec) 
    throws Exception {
        String schema = null;

        String objectOwner = null;
        String objectName  = null;

        Map<EntryTagType, List<EntryTagRecord>> tags = rec.getEntryTags();
        EntryTagRecord tag = null;
        int objectId = -1;

        /* object ID must always be present */
        EntryTagType type = EntryTagType.TAG_OBJ_ID;

        if (rec.hasOwnerMetaData()) {
            if (tags.containsKey(type)) {
                tag = tags.get (type).get(0);
                objectId = 
                    SimpleDataDecoder.decodeInteger(tag.getRawData());
            }
            else {
                throw new Exception (
                    "Unable to process a PLOG data record of type: " +
                    rec.getSubType() + " with no object ID"
                );
            }

            if (schemaCache.containsKey(objectId)) {
                /* have cache */
                schema = schemaCache.get(objectId);
            }
            else {
                type = EntryTagType.TAG_OBJ_OWNER;
                if (tags.containsKey(type)) {
                    tag = tags.get (type).get(0);
                    objectOwner = 
                        SimpleDataDecoder.decodeCharString(tag.getRawData());
                }
                else if (logger.isTraceEnabled()) {
                    /* only for debugging, this is fine if the filtered
                     * stream is not interested in this table and forwarded
                     * past it's schema definition record in the PLOG 
                     * data stream
                     */
                    logger.trace (
                        "[SCN: " + rec.getRecordSCN() + "] " + 
                        "No object owner for object ID: "    + objectId 
                    );
                }

                type = EntryTagType.TAG_OBJ_NAME;
                if (tags.containsKey(type)) {
                    tag = tags.get (type).get(0);
                    objectName =
                        SimpleDataDecoder.decodeCharString(tag.getRawData());
                }
                else if (logger.isTraceEnabled()) {
                    logger.trace (
                        "[SCN: " + rec.getRecordSCN() + "] " + 
                        "No object name for object ID: "     + objectId 
                    );
                }

                if (objectOwner != null && objectName != null) {
                    schema = 
                        objectOwner.replace("$", "") + "." + 
                        objectName.replace("$", "");

                    /* add to local cache because we are not able to use PLOG cache
                     * in case these are internally filtered schemas
                     */
                    schemaCache.put (objectId, schema);
                }
            }
        }

        rec.setOwner(schema);
    }

    /** 
     * Helper function to populate the SCN of a raw PLOG record to 
     * support <i>FilterableRecord</i> interface and criteria
     * 
     * @param plog PLOG file meta data
     */
    private void parseEntryRecordSCN (PlogFile plog, EntryRecord rec) 
    throws Exception {
        Map<EntryTagType, List<EntryTagRecord>> tags = rec.getEntryTags();
        EntryTagType type  = EntryTagType.TAG_OBJ_OWNER;
        EntryTagRecord tag = null;

        type = EntryTagType.TAG_SCN;
        if (tags.containsKey(type)) {
            tag = tags.get (type).get(0);
            rec.setSCN(
                SimpleDataDecoder.decodeLong (tag.getRawData())
            );
        }
    }

}
