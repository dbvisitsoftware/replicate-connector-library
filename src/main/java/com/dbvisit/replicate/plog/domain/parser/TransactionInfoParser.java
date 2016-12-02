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

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.plog.domain.DomainRecord;
import com.dbvisit.replicate.plog.domain.TransactionInfoRecord;
import com.dbvisit.replicate.plog.file.PlogFile;
import com.dbvisit.replicate.plog.format.EntryRecord;
import com.dbvisit.replicate.plog.format.EntryTagRecord;
import com.dbvisit.replicate.plog.format.EntryTagType;
import com.dbvisit.replicate.plog.format.decoder.SimpleDataDecoder;


/**
 * Parse and aggregate individual PLOG transaction entries as transaction
 * information records
 */
public class TransactionInfoParser implements DomainParser {
    private static final Logger logger = LoggerFactory.getLogger(
        TransactionInfoParser.class
    );

    /** The transaction data record to emit */
    private TransactionInfoRecord txr;
    
    /** The current transaction data record being parsed */
    private TransactionInfoRecord currentTxr;
    
    /** Ignore partials when they are merged with parent records */
    private boolean ignorePartials = false;
    
    /* Dictionary parser */
    final DictionaryParser dictionaryParser = new DictionaryParser();
    
    /**
     * Parse a transaction data record, this is an aggregation of LCR 
     * properties
     * 
     * @param plog PLOG file
     * @param rec  PLOG entry record
     * 
     * @throws Exception Failed to parse a PLOG entry record to a Transaction
     *                   data record
     */
    @Override
    public void parse(PlogFile plog, EntryRecord rec) throws Exception {
        try {
            /* decode core fields from LCR use to build up transaction info */
            Map <EntryTagType, List<EntryTagRecord>> tags = rec.getEntryTags();
            
            EntryTagRecord tag;
            int plogId = plog.getId();
            long lcrId = 0L;
            String txId = null;
            long scn = 0L;
            Timestamp time = null;
            
            /* parse dictionary and maintain cache, if needed */
            dictionaryParser.parse(plog, rec);
            
            /* first parse transaction ID only */
            EntryTagType type = EntryTagType.TAG_XID;
            if (tags.containsKey(type)) {
                tag = tags.get (type).get(0);
                txId = SimpleDataDecoder.decodeCharString(tag.getRawData());
            }
            
            /* do not process records with no transaction ID and omit
             * partial records from aggregation when parent LCRs are
             * merged
             */
            if (txId != null && 
                ((ignorePartials && !rec.isPartialRecord()) 
                  || !ignorePartials)
               )
            {
                /* check if we have a new transaction */
                if (currentTxr != null &&
                    currentTxr.getId() != null &&
                    !currentTxr.getId().equals (txId)) 
                {
                    if (currentTxr.isValid()) {
                        /* a new transaction has started, emit */
                        currentTxr.setComplete(true);
                        txr = currentTxr;
                    }
                    else {
                        /* no need to retain previous invalid tx info */
                        currentTxr.removeFromPlogCache();
                    }

                    /* create new record and add it to cache */
                    currentTxr = new TransactionInfoRecord ();
                    currentTxr.setId (txId);
                    plog.addTransactionRecordToCache(currentTxr);
                }
                
                TransactionInfoRecord cachedTxr = 
                    plog.getTransactionRecordFromCache(txId);
                
                if (cachedTxr == null) {
                    /* no cache, create new TXR and add it to cache */
                    currentTxr = new TransactionInfoRecord ();
                    currentTxr.setId (txId);
                    plog.addTransactionRecordToCache(currentTxr);
                }
                else {
                    /* have cached record */
                    currentTxr = cachedTxr;
                }
                
                type = EntryTagType.TAG_PLOGSEQ;
                if (tags.containsKey(type)) {
                    tag = tags.get (type).get(0);
                    plogId = SimpleDataDecoder.decodeInteger (tag.getRawData());
                }

                type = EntryTagType.TAG_LCR_ID;
                if (tags.containsKey(type)) {
                    tag = tags.get (type).get(0);
                    lcrId = SimpleDataDecoder.decodeLong(tag.getRawData()) 
                            + (1000000000L * plogId);
                    
                    /* LCR ids */
                    if (currentTxr.getStartRecordId() == 0L ||
                        lcrId < currentTxr.getStartRecordId())
                    {
                        currentTxr.setStartRecordId(lcrId);
                    }
                    if (currentTxr.getEndRecordId() == 0L ||
                        lcrId > currentTxr.getEndRecordId())
                    {
                        currentTxr.setEndRecordId(lcrId);
                    }
                }
                
                if (logger.isTraceEnabled()) {
                    logger.trace (
                        "PLOG id: " + plogId + " " +
                        "tx count: " + plog.getTransactionRecords().size() + " " +
                        "tx id: "   + txId   + " " +
                        "lcr id: "  + lcrId
                    );
                }

                /* TAG_SAVEPOINT_ID: not needed for pessimistic commit */

                type = EntryTagType.TAG_SCN;
                if (tags.containsKey(type)) {
                    tag = tags.get (type).get(0);
                    scn = SimpleDataDecoder.decodeLong (tag.getRawData());
                    
                    /* SCNs */
                    if (currentTxr.getStartSCN() == 0L ||
                        scn < currentTxr.getStartSCN())
                    {
                        currentTxr.setStartSCN(scn);
                    }
                    if (currentTxr.getEndSCN() == 0L ||
                        scn > currentTxr.getEndSCN())
                    {
                        currentTxr.setEndSCN(scn);
                    }
                }

                type = EntryTagType.TAG_DTIME;
                if (tags.containsKey(type)) {
                    tag = tags.get (type).get(0);
                    time = SimpleDataDecoder.decodeDate(tag.getRawData());
                    
                    /* time */
                    if (currentTxr.getStartTime() == null ||
                        time.before (currentTxr.getStartTime()))
                    {
                        currentTxr.setStartTime(time);
                    }
                    if (currentTxr.getEndTime() == null ||
                        time.after(currentTxr.getEndTime()))
                    {
                        currentTxr.setEndTime(time);
                    }
                }

                /* parse start/end fields, PLOG ids, either read from PLOG 
                 * file or PLOG entry records
                 */
                if (currentTxr.getStartPlogId() == 0 ||
                    plogId < currentTxr.getStartPlogId())
                {
                    currentTxr.setStartPlogId(plogId);
                }
                if (currentTxr.getEndPlogId() == 0 ||
                    plogId > currentTxr.getEndPlogId())
                {
                    currentTxr.setEndPlogId(plogId);
                }

                if (rec.isDataRecord()) {
                    /* only count data LCRs */
                    currentTxr.incrementRecordCount();
                    
                    /* count data LCRs per schema */
                    currentTxr.incrementSchemaRecordCount(rec.getRecordSchema());
                }

                /* for transactions we need to aggregate all of it's
                 * constituent records raw size
                 */
                currentTxr.setRawRecordSize(
                    currentTxr.getRawRecordSize() + rec.getSize()
                );
            }
        } catch (Exception e) {
            throw new Exception (
                "Failed to build TX info record, reason: " + e.getMessage(),
                e
            );
        }
    }
    
    /** 
     * Emit the parsed and complete aggregate transaction record, return it
     * to caller and remove it from cache
     * 
     * @return transaction information domain record
     */
    @Override
    public DomainRecord emit() {
        TransactionInfoRecord result = null;
        if (txr != null && txr.isComplete()) {
            result = txr;
            txr.removeFromPlogCache();
            txr = null;
        }
        return result;
    }

    /**
     * Check if transaction information record is complete and ready
     * to use
     * 
     * @return true if current record is ready to emitted, else false if 
     *              it's still being aggregated
     */
    @Override
    public boolean canEmit() {
        return txr != null && txr.isComplete();
    }

    /** 
     * For multi-part records the partial records and their transactions
     * are ignored, because they are merged in output
     * 
     * @return true, support merged records by ignoring partials produced
     */
    @Override
    public boolean supportMultiPartMerging() {
        return true;
    }

    /**
     * Handle multi-part records by ignoring partial records and their
     * transactions
     * 
     * @throws Exception to conform
     */
    @Override
    public void enableMultiPartMerging() throws Exception {
        this.ignorePartials = true;
    }

    /**
     * Transaction information is aggregated from transaction, audit
     * and data change records
     * 
     * @return true, this is an aggregate parser
     */
    @Override
    public boolean isAggregateParser() {
        /* tx info is aggregated from other records */
        return true;
    }

}
