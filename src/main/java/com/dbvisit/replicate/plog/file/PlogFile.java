package com.dbvisit.replicate.plog.file;

import java.io.DataInputStream;

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
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.plog.domain.HeaderRecord;
import com.dbvisit.replicate.plog.domain.ChangeRowRecord;
import com.dbvisit.replicate.plog.domain.TransactionInfoRecord;
import com.dbvisit.replicate.plog.metadata.DDLMetaData;
import com.dbvisit.replicate.plog.metadata.Table;
import com.dbvisit.replicate.plog.reader.DomainReader;
import com.dbvisit.replicate.plog.reader.PlogStreamReader;

/** 
 * Manages a single Processed Log (PLOG) file, knows how to identify a 
 * PLOG file, what it means to be valid, how to open it, close and who
 * should read it. The PLOG sequence corresponds to REDO sequence.
 */
public class PlogFile {
    private static final Logger logger = LoggerFactory.getLogger(
        PlogFile.class
    );
    
    public static Logger getLogger() {
        return logger;
    }
    
    /** Offset in bytes for start of PLOG entry control header record */
    public static final int PLOG_HEADER_BYTE_OFFSET = 16;
    /** Offset in bytes for start of PLOG entry data records */
    public static final int PLOG_DATA_BYTE_OFFSET = 112;
    /** Size in bytes of a PLOG chunk */
    public static final int PLOG_DATA_CHUNK_BYTES = 4;
    /** PLOG file extension */
    public static final String PLOG_EXT = "plog";
    /** Default shift for PLOG ID in UID */
    public static final long PLOG_ID_UID_BITSHIFT = 32;

    /** Major PLOG number, correspond to REDO sequence number, as ID */
    private int id;
    /** Minor PLOG number, as load ID */
    private int loadId;
    /** Epoch timestamp when PLOG was created */
    private int timestamp;
    /** Base directory of MINE */
    private String baseDirectory;
    /** Filename of this PLOG on disk */
    private String fileName;
    /** Each PLOG has it's own PLOG stream reader */
    private final PlogStreamReader reader;
    /** Cache table dictionary information for compactly encoded PLOGs */
    private Map<Integer, Table> dictionary;
    /** Cache schema definitions for replicated data sets */
    private Map<String, DDLMetaData> schemas;
    /** Flag to indicate whether or not schema definitions was updated to
     *  a newer version */
    private boolean updatedSchema = false;
    /** Keep track of partial LCRs for multi-part updates */
    private Map<String, ChangeRowRecord> partialRecords;
    /** Keep track of current transactions */
    private Map<String, TransactionInfoRecord> transactionRecords;
    /** Header record stores the features encoded in PLOG */
    private HeaderRecord header;
    /** 
     * Whether or not to forcefully close this PLOG at the end boundary, 
     * this is required for corrupted PLOGs created during a restart. 
     * A valid PLOG must follow, use with care
     */
    private boolean forceCloseAtEnd;
    
    /**
     * Create and initialize PLOG file from defaults with configured domain
     * reader
     * 
     * @param domainReader the domain reader to use for converting the entries
     *                     in this PLOG file to domain records
     * @throws Exception when initialisation error occur
     */
    public PlogFile (final DomainReader domainReader) throws Exception {
        reader = new PlogStreamReader(this, domainReader);
        init ();
    }
    
    /**
     * Create and initialize PLOG file from stream with configured domain
     * reader for converting entries in stream to PLOG domain records
     * 
     * @param domainReader the domain reader to use for converting the entries
     *                     in this PLOG file to domain records
     * @param stream       pre-initialised data input stream of LCRs
     * @throws Exception when initialisation error occur
     */
    public PlogFile (
        final DomainReader domainReader,
        final DataInputStream stream
    ) throws Exception {
        reader = new PlogStreamReader(this, domainReader, stream);
        init ();
    }
    
    /**
     * Create and initialize a PLOG file using replication sequence number,
     * time created and full path to on-disk PLOGs
     * 
     * @param id        The ID, REDO sequence number, of PLOG to open from
     *                  disk
     * @param timestamp The epoch timestamp when PLOG was created, this
     *                  together with REDO sequence number uniquely 
     *                  identifies a PLOG
     * @param fullPath  Full path to on disk location of replicate MINE's 
     *                  output
     * @param domainReader The domain reader to use for converting the entries
     *                  in this PLOG file to domain records                 
     *                  
     * @throws Exception if unique ID fields and file name do not match
     */
    public PlogFile (
        int    id,
        int    timestamp,
        String fullPath,
        final  DomainReader domainReader
    ) throws Exception
    {
        this.id        = id;
        this.timestamp = timestamp;
        baseDirectory  = fullPath.substring (0, fullPath.lastIndexOf('/'));
        fileName       = fullPath.substring (fullPath.lastIndexOf('/') + 1);
    
        if (!fileName.equals (getFileNameFromUID(getUID()))) {
            throw new Exception (
                "PLOG file name: " + fileName + " do not match the " +
                "REDO log sequence number: "  + id + " and "         +
                "timestamp: " + timestamp     + " provided"
            );
        }
        reader = new PlogStreamReader(this, domainReader);
        init ();
    }
    
    /**
     * Create and initialize a PLOG file using sequence number of PLOG in
     * replication, time created, the base directory to the output of 
     * replicate MINE and the file name of PLOG file to open on disk
     * 
     * @param id            The ID sequence number of PLOG in replication
     * @param timestamp     The epoch timestamp when PLOG was created
     * @param baseDirectory The root directory of replicate MINE's output
     * @param fileName      The file name of PLOG file on disk
     * @param domainReader  The domain reader to use for converting the entries
     *                      in this PLOG file to domain records
     * @throws Exception if unique ID fields and file name do not match
     */
    public PlogFile (
        int    id, 
        int    timestamp, 
        String baseDirectory, 
        String fileName,
        final  DomainReader domainReader
    ) throws Exception 
    {
        this.id            = id;
        this.timestamp     = timestamp;
        this.baseDirectory = baseDirectory;
        this.fileName      = fileName;

        if (!fileName.equals (getFileNameFromUID(getUID()))) {
            throw new Exception (
                "PLOG file name: " + fileName + " do not match the " +
                "REDO log sequence number: "  + id + " and "         +
                "timestamp: " + timestamp     + " provided"
            );
        }
        reader = new PlogStreamReader(this, domainReader);
        init ();
    }
    
    /**
     * Create and initialize a LOAD PLOG file using the parent's PLOGs 
     * sequence number in replication, time created, the sub-sequence in 
     * included load files, the base directory of replicate MINE and the
     * file name of the LOAD (child) PLOG to open
     * 
     * @param id            The ID sequence of parent PLOG in replication
     * @param loadId        The load ID of included child PLOG file in
     *                      sub-stream of replication
     * @param timestamp     The epoch timestamp when PLOG was created                      
     * @param baseDirectory The base directory of replicated output
     * @param fileName      The name of PLOG file on disk
     * @param domainReader  The domain reader to use for converting the entries
     *                       in this PLOG file to domain records
     * @throws Exception when initialisation error occur
     */
    public PlogFile (
        int    id,
        int    loadId,
        int    timestamp,
        String baseDirectory,
        String fileName,
        final  DomainReader domainReader
    ) throws Exception {
        this.id            = id;
        this.loadId        = loadId;
        this.timestamp     = timestamp;
        this.baseDirectory = baseDirectory;
        this.fileName      = fileName;
    
        reader = new PlogStreamReader(this, domainReader);
        init ();
    }
    
    /** Initialize PLOG cache */
    private void init () {
        dictionary = new HashMap<Integer, Table>();
        schemas = new HashMap<String, DDLMetaData>();
        partialRecords = new HashMap <String, ChangeRowRecord>();
        transactionRecords = 
            new LinkedHashMap <String, TransactionInfoRecord>();
        forceCloseAtEnd = false;
    }

    /**
     * Set the ID of the PLOG file which is the unique sequence number of
     * the PLOG in replication
     * 
     * @param id the sequence number (ID) that identifies this PLOGs position
     *           in replication
     */
    public void setId (int id) {
        this.id = id;
    }
    
    /**
     * Return the ID of the PLOG file in the sequence of PLOGs in replication
     * 
     * @return unique ID of PLOG in replication sequence
     */
    public int getId () {
        return this.id;
    }
    
    /**
     * Set the sub-sequence of included PLOG file to LOAD
     * 
     * @param loadId sub-sequence assigned to LOAD (child) PLOG as ID
     */
    public void setLoadId (int loadId) {
        this.loadId = loadId;
    }
    
    /**
     * Return the ID of the LOAD PLOG in the sequence of included child PLOGS
     * that need to be loaded at given SCN
     * 
     * @return ID of child PLOG in LOAD sequence
     */
    public int getLoadId () {
        return this.loadId;
    }
    
    /**
     * Set the time when PLOG file was created, as epoch time.
     * 
     * @param timestamp epoch time of when PLOG was created
     */
    public void setTimestamp (int timestamp) {
        this.timestamp = timestamp;
    }
    
    /*
     * Return the epoch time of when the PLOG file was created on disk, this
     * forms part of its unique identifier
     * 
     * @return epoch time of when the PLOG file was created
     */
    public int getTimestamp () {
        return this.timestamp;
    }
    
    /**
     * Set the name of PLOG file on disk. 
     * 
     * <p>
     * For replicate connector a normal PLOG filename consist of 
     * <em>PLOG ID</em>.plog.<em>time stamp</em>, eg.
     * 746.plog.1476671651 and for LOAD PLOGS the sub-sequence and table 
     * information is appended eg. 
     * -<em>LOAD ID</em>-LOAD_<em>TABLE ID</em>-<em>OWNER</em>.<em>TABLE</em>-APPLY
     * </p>
     * 
     * @param fileName file name of PLOG file on disk
     */
    public void setFileName (String fileName) {
        this.fileName = fileName;
    }
    
    /**
     * Return the name of the PLOG file on disk
     * 
     * @return file name of PLOG file
     */
    public String getFileName () {
        return this.fileName;
    }
    
    /**
     * Set the base (root) directory containing all PLOGs produced by
     * replicate MINE. This is done so that children PLOGs inherit their
     * location from their parent PLOGs.
     * 
     * @param baseDirectory root directory of replicated output, as
     *                      absolute path
     */
    public void setBaseDirectory (String baseDirectory) {
        this.baseDirectory = baseDirectory;
    }
    
    /**
     * Return the root directory of replicated output of processed LOGs.
     * 
     * @return absolute path to base directory of replicate MINE
     */
    public String getBaseDirectory () {
        return this.baseDirectory;
    }
    
    /**
     * Return the full path to the PLOG on disk
     * 
     * @return full path to PLOG
     */
    public String getFullPath () {
        return baseDirectory + '/' + fileName; 
    }
    
    /**
     * Return the reader that is used by PLOG for reading it's contents
     * 
     * @return PLOG reader
     */
    public PlogStreamReader getReader () {
        return this.reader;
    }
    
    /**
     * Return whether or not the PLOG's control header has compact encoding
     * enabled, as in the dictionary information is not present on every
     * row change record
     * 
     * @return true if PLOG has compact dictionary information, else false
     */
    public boolean isCompact () {
        return header.hasCompactEncoding();
    }
    
    /**
     * This is used by some clients to keep track of when a new updated
     * schema was emitted in PLOG file which should trigger down stream
     * updates before processing any change records
     * 
     * @param updatedSchema true if any schema in the cache have been updated.
     *                      else false
     */
    public void setUpdatedSchema (boolean updatedSchema) {
        this.updatedSchema = updatedSchema;
    }
    
    /**
     * Return whether or not any of the schemas in the cache has been
     * updated or modified by incoming data definition change
     * 
     * @return true if a newer schema definition is available, else false
     */
    public boolean hasUpdatedSchema () {
        return this.updatedSchema;
    }
    
    /**
     * Allow adding a pre-build dictionary cache, as when a previous PLOG
     * in replicated stream passes it's cache on
     * 
     * @param dictionary lookup of Table meta data by table object ID
     */
    public void setDictionary (Map<Integer, Table> dictionary) {
        this.dictionary = dictionary;
    }
    
    /**
     * Return the table meta data dictionary
     * 
     * @return lookup of Table dictionary by table object ID
     */
    public Map<Integer, Table> getDictionary () {
        return this.dictionary;
    }
    
    /**
     * Allow adding a pre-build schema cache containing lookup of schema
     * meta data by fully qualified table name, usually set from
     * previous PLOG in sequence
     * 
     * @param schemas lookup of schema meta data by schema identifier
     */
    public void setSchemas (Map<String, DDLMetaData> schemas) {
        this.schemas = schemas;
    }
    
    /**
     * Return the schema meta data for all schemas present in PLOG stream
     * so far
     * 
     * @return schema meta data, lookup by schema identifier
     */
    public Map<String, DDLMetaData> getSchemas () {
        return this.schemas;
    }
    
    /**
     * Set the partial change records present in previous PLOG, need to
     * maintain these if LOB writes span PLOG boundaries
     * 
     * @param partialLCRs these are a cache of previous multi-part LCRs
     *                    of LOB writes, the lookup is done by the
     *                    supplementally logged key
     */
    public void setPartialRecords (
        Map<String, ChangeRowRecord> partialLCRs
    ) {
        this.partialRecords = partialLCRs;
    }
    
    /**
     * Return the cache of partial LOB LCRs to pass on to next PLOG in
     * replicate sequence
     * 
     * @return the partial LCRs in this PLOG
     */
    public Map<String, ChangeRowRecord> getPartialRecords () {
        return this.partialRecords;
    }
    
    /**
     * Clear out the cache of partial LCRs when done
     */
    public void clearPartialRecords () {
        if (partialRecords != null) {
            partialRecords.clear();
        }
    }
    
    /**
     * Set the cache of aggregate transaction information records using
     * cache maintained by previous PLOG in replicate sequence
     * 
     * @param transactionRecords previous PLOGs cache of transaction aggregate
     *                           records, lookup by transaction ID
     */
    public void setTransactionRecords (
        Map<String, TransactionInfoRecord> transactionRecords
    ) {
        this.transactionRecords = transactionRecords;
    }

    /**
     * Return this PLOGs cache of transaction aggregate records
     * 
     * @return transaction aggregate records, lookup by transaction ID
     */
    public Map<String, TransactionInfoRecord> getTransactionRecords () {
        return this.transactionRecords;
    }
    
    /**
     * When done, this PLOG should clear out it's version of the transaction
     * information record cache
     */
    public void clearTransactionRecords () {
        synchronized (transactionRecords) {
            if (transactionRecords != null) {
                transactionRecords.clear();
            }
        }
    }
    
    /**
     * Allow cache lookup for transaction aggregate record by transaction
     * ID, this allows aggregating LCRs properties in transactions that
     * span PLOG boundaries
     * 
     * @param txId the transaction ID to find aggregate record for
     * 
     * @return the aggregate record in cache when transactions spans PLOG
     *         boundaries or NULL if none present 
     */
    public TransactionInfoRecord getTransactionRecordFromCache (String txId) {
        TransactionInfoRecord cacheHit = null;
        
        synchronized (transactionRecords) {
            if (transactionRecords != null &&
                transactionRecords.containsKey (txId))
            {
                cacheHit = transactionRecords.get (txId);
            }
        }
        
        return cacheHit;
    }
    
    /**
     * Add a new and incomplete transaction information record to this
     * PLOGs cache, to allow aggregation of LCR properties
     * 
     * @param txr the incomplete, parsed transaction information record
     */
    public void addTransactionRecordToCache (TransactionInfoRecord txr) {
        synchronized (transactionRecords) {
            if (transactionRecords == null) {
                transactionRecords = 
                    new LinkedHashMap <String, TransactionInfoRecord>();
            }
            
            transactionRecords.put (txr.getId(), txr);
            /* circular reference */
            txr.setPlog(this);
        }
    }
    
    /**
     * Remove a transaction information record from this PLOGs cache, either
     * it's complete and have been emitted to domain reader or it's a partial
     * and has been emitted at the end of PLOG forcefully
     * 
     * @param txr the emitted transaction information to remove from local
     *            cache 
     */
    public void removeTransactionRecordFromCache (TransactionInfoRecord txr) {
        synchronized (transactionRecords) {
            if (transactionRecords != null) {
                transactionRecords.remove(txr.getId());
                /* circular reference */
                txr.setPlog (null);
            }
        }
    }
    
    /**
     * Set the control header record parsed for this PLOG file, this is done
     * when PLOG's reader opens it's data stream
     * 
     * @param header the parsed header record
     */
    public void setHeader (HeaderRecord header) {
        this.header = header;
    }
    
    /**
     * Return the control header record parsed for this PLOG, it contains
     * information needed to decode LCRs and the feature set
     * 
     * @return PLOG's header record
     */
    public HeaderRecord getHeader () {
        return this.header;
    }
    
    /**
     * Open PLOG file, it's reader and it's data stream, only if the PLOG
     * file can be used and is ready. Opening the stream reader results in
     * basic setup done to the PLOG file by reading PLOG file header and
     * control header to determine how to decode certain domain records
     * 
     * @throws Exception if unable to open the PLOG file for reading
     */
    public void open () throws Exception {
        boolean opened = canUse();
        if (!opened) {
            reader.open();
        }        
    }
    
    /**
     * Close this PLOG file, it's data stream, reader and clear it's cache
     * 
     * @return true if was closed, else false if it failed to close which
     *         is allowed, let the caller decide what to do about it
     */
    public boolean close () {
        boolean closed = true;
        if (reader != null) {
            try {
                reader.close();
            }
            catch (Exception e) {
                
                logger.error (
                    "Could not close data stream on PLOG: " + fileName +
                    ", reason: " + e.getMessage()
                );
                closed = false;
            }
        }
        clear();
        
        return closed;
    }
    
    /** 
     * Check whether or not this PLOG file can be used for reading, either
     * is ready to start, busy reading and not done
     * 
     * @return true if this PLOG file can be used for reading
     */
    public boolean canUse () {
        boolean use = false;
        
        if (reader != null) {
            use = (reader.isReady() ||
                   reader.isBusy()) &&
                   !reader.isDone();
        }
        
        return use;
    }
    
    /**
     * Simple string representation for logging
     * 
     * @return String textual description of PLOG
     */
    public String toString () {
        return 
            "PLOG ID: " + id + " " +
            (loadId > 0 ? "load ID: " + loadId + " " : "") + 
            "file: " + fileName + " " + 
            "ready: " + canUse();
    }
    
    /**
     * Copy the cache from previous PLOG in replication sequence to the current
     * opened PLOG
     * 
     * @param copyFrom previous PLOG in replication sequence with valid cache
     */
    public void copyCacheFrom (PlogFile copyFrom) {
        if (copyFrom.getSchemas() != null) {
            schemas.putAll (copyFrom.getSchemas());
        }
        
        if (copyFrom.getDictionary() != null) {
            dictionary.putAll (copyFrom.getDictionary());
        }
        
        if (copyFrom.getPartialRecords() != null) {
            partialRecords.putAll (copyFrom.getPartialRecords());
        }
        
        if (copyFrom.getTransactionRecords() != null) {
            transactionRecords.putAll (copyFrom.getTransactionRecords());
        }
    }
    
    /** 
     * Check whether or not this PLOG has a valid header and can be parsed
     * 
     * @return true if valid, else false
     */
    public boolean isValid () {
        return header.isValid();
    }
    
    /**
     * Clear out cache
     */
    private void clear () {
        schemas.clear();
        dictionary.clear();
        partialRecords.clear();
        transactionRecords.clear();
    }
    
    /**
     * Identify a LOAD (child) PLOG by presence of valid load ID
     * 
     * @return true if this PLOG is a LOAD (included file) PLOG, else false
     */
    public boolean isLoadFile () {
        return loadId > 0;
    }

    /**
     * Enable the ability for domain reader to forcefully close a PLOG
     * stream. When this option is set the domain reader will assume that
     * the PLOG file was corrupted, as in not finalized (no PLOG footer),
     * during a replication service restart and will never end. To avoid
     * waiting for EOF we instead wait for pre-defined number of wait
     * intervals, eg. 60 waits of 1000ms each, before forcing a stream
     * closure. This is only done for the PLOG file that was identified
     * by <em>PlogFileManager</em> as a boundary PLOG during restart.
     */
    public void enableForceCloseAtEnd () {
       forceCloseAtEnd = true;
    }
    
    /**
     * Check whether or not the data stream for this PLOG may be forcefully
     * closed after waiting for new data to arrive at EOF for a pre-defined
     * time out value. This is only intended for forcefully closing a 
     * partial, unfinished PLOG that was meant to be closed but wasn't
     * during a restart of the replicate service
     * 
     * @return true if the domain reader can close the PLOG stream if
     *         EOF wait time out occurs, else false
     */
    public boolean forceCloseAtEnd () {
        return forceCloseAtEnd;
    }
    
    /**
     * Return UUID of parent MINE process that created this PLOG file
     * 
     * @return MINE UUID
     */
    public String getMineUUID () {
        return header != null 
               ? header.getMineUUID() 
               : null;
    }
    
    /**
     * Get unique ID for PLOG composed of the REDO log sequence number
     * and time PLOG was created
     * 
     * @return unique ID for PLOG, as long value
     */
    public long getUID () {
        return ((long)id << PLOG_ID_UID_BITSHIFT) | timestamp;
    }
    
    /**
     * Helper function to recover the name of the PLOG file that this UID
     * was created from
     * 
     * @param uid PLOG unique ID composed of REDO sequence and time created
     * 
     * @return PLOG base filename determined from UID
     */
    public static String getFileNameFromUID (long uid) {
        int id        = (int)((uid >> PLOG_ID_UID_BITSHIFT) & 0xFFFFFFFF);
        int timestamp = (int)(uid & 0xFFFFFFFF);
        
        return id + "." + PLOG_EXT + "." + timestamp;
    }
    
    /**
     * Helper function to get the ID of a PLOG from its unique ID, 
     * which is a composite of its PLOG ID and time stamp
     * 
     * @param uid PLOG unique ID composed of REDO sequence and time created
     * 
     * @return ID, redo sequence number, for PLOG
     */
    public static int getPlogIdFromUID (long uid) {
        return (int)((uid >> PLOG_ID_UID_BITSHIFT) & 0xFFFFFFFF);
    }
    
    /**
     * Helper function to get the time stamp for a PLOG from its 
     * unique ID, which is a composite of its PLOG ID and time stamp
     * 
     * @param uid PLOG unique ID composed of REDO sequence and time created
     * 
     * @return time stamp for PLOG file
     */
    public static int getPlogTimestampFromUID (long uid) {
        return (int)(uid & 0xFFFFFFFF);
    }
    
    /**
     * Helper function to create a PLOG UID from a PLOG ID (REDO logs sequence
     * number) and time stamp 
     * 
     * @param id        PLOG ID from REDO log sequence number
     * @param timestamp PLOG time stamp when it was created
     * 
     * @return composite unique ID to uniquely identify a PLOG file
     */
    public static long createPlogUID (int id, int timestamp) {
        return ((long)id << PLOG_ID_UID_BITSHIFT) | timestamp;
    }
    
}
