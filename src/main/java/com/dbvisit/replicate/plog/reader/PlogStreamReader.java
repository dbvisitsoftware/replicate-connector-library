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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.plog.domain.DomainRecord;
import com.dbvisit.replicate.plog.domain.HeaderRecord;
import com.dbvisit.replicate.plog.domain.parser.HeaderParser;
import com.dbvisit.replicate.plog.file.PlogFile;
import com.dbvisit.replicate.plog.format.EntryRecord;
import com.dbvisit.replicate.plog.format.PlogHeader;
import com.dbvisit.replicate.plog.format.parser.EntryRecordParser;
import com.dbvisit.replicate.plog.format.parser.PlogHeaderParser;

/** 
 * Manages the reading from the PLOG as stream, it knows nothing about the 
 * contents of the PLOG file, it only treats it like a stream of bytes. It
 * keeps tracks of the byte offset within the stream, allow marking an offset,
 * and rewinding to markers or forwarding to given offsets. It defers the
 * actual work of interpreting the stream of bytes as PLOG entry records
 * and converting it to domain objects to a domain reader, it only maintains
 * a cache from the domain records parsed. 
 * 
 * <p>
 * The state or read behavior of this class should should not be changed or 
 * shared, one domain reader per PLOG stream, each opened on its own version
 * of PLOG file handle. For now it is not enforced, change this to be 
 * immutable and created by builder. Remove public API used by testing.
 * </p>
 */
public class PlogStreamReader {
    private static final Logger logger = LoggerFactory.getLogger(
        PlogStreamReader.class
    );
    
    /** Default flush size of domain cache  */
    private static final int FLUSH_SIZE = 1000;
    /** Default read limit on stream */
    private static final int READ_LIMIT = 1000000;
    /** The buffered input bytes stream that will be opened on PLOG file */
    private DataInputStream plogStream;
    /** Identify if this stream reader is ready for reading records */
    private boolean ready = false;
    /** Identify if this stream reader is done reading */
    private boolean done  = false;
    /** The parent PLOG of this stream reader */
    private PlogFile plog;
    /** The child domain reader to use for reading domain records from PLOG */
    private DomainReader domainReader;
    /** The maximum size of domain cache before it will be flushed */
    private int flushSize = FLUSH_SIZE;
    /** Number of records read in PLOG */
    private int recordCount = 0;
    final private List<DomainRecord> data;
    /** Byte offset of reader, the last byte read */
    private long offset = 0L;
    /** Byte offset of marker in stream, used for rewinding */
    private long marker = 0L;
    /** Acting as proxy or sub-stream of parent, offsets are static */
    private boolean proxy = false;
    /** Paused by proxy, this is the parent */
    private boolean paused = false;

    /**
     * Create and initialize PLOG stream reader from PLOG file handle
     * 
     * @param plog PLOG file to open and read
     */
    public PlogStreamReader (PlogFile plog) {
        this.plog = plog;
        domainReader = new DomainReader();
        data = new LinkedList<DomainRecord>();

        if (plog.isLoadFile()) {
            proxy = true;
        }
    }

    /**
     * Create and initialize empty PLOG stream reader
     */
    public PlogStreamReader () {
        domainReader = new DomainReader();
        data = new LinkedList<DomainRecord>();
    }

    /**
     * Manually set the data input stream to process, the data stream must
     * have already been opened on a valid PLOG file.
     * 
     * @param plogStream data input stream already opened on PLOG
     */
    public void setPlogStream (DataInputStream plogStream) {
        this.plogStream = plogStream;
    }

    /**
     * Return the current data input stream for reading for a PLOG file
     * 
     * @return buffered data input stream opened on a valid PLOG file
     */
    public final DataInputStream getPlogStream () {
        return this.plogStream;
    }

    /** 
     * Prepare the PLOG stream for reading, setting it's internal state
     * accordingly. It opens the PLOG file stream, parsing the file header
     * and validating that it is a valid PLOG file after which it parses
     * the PLOG header record that holds the properties of the PLOG file
     * required before reading records from it. The stream reader is ready
     * if PLOG stream is valid and stream is open.
     * 
     * @throws Exception Failed to prepare the data stream
     */
    public void prepare () throws Exception {
        if (plogStream == null) {
            throw new Exception (
                "The PLOG stream has not been opened on PLOG: " + 
                plog.getFileName()
            );
        }
        
        try {
            if (isValid()) {
                setReady (true);
                readPlogFeatures ();
            }
            else {
                /* not a PLOG file, abort */
                throw new Exception (
                    "Invalid file: " + plog.getFileName() + ", reason: " +
                    "it is not a valid PLOG file"
                );
            }
        } catch (Exception e) {
            /* an unexpected error has occurred whilst attempting to parse
             * the PLOG file header, close stream before passing it on
             */
            close();
            
            throw e;
        }
    }

    /** 
     * Set whether or not this PLOG stream reader has been prepared and is
     * ready to read records
     * 
     * @param ready true, if ready, else false
     */
    private void setReady (boolean ready) {
        this.ready = ready;
    }

    /**
     * Return whether or not the stream is ready for parsing and reading
     * domain records
     * 
     * @return boolean true if this PLOG stream reader is ready to parse, else
     *                 return false
     */
    public boolean isReady () {
        return this.ready;
    }

    /**
     * Set whether or not the PLOG reader is done, as in has read until the
     * EOF and have parsed a valid PLOG footer record
     * 
     * @param done true if done, else false
     */
    public void setDone (boolean done) {
        if (done) {
            /* it's done, cannot be ready */
            ready = false;
        }
        this.done = done;
    }

    /**
     * Return whether or not this reader is done with reading records from its
     * PLOG file
     * 
     * @return true if done, else false
     */
    public boolean isDone () {
        return this.done;
    }

    /**
     * Set the PLOG file for which this reader is managing as a stream of bytes
     * 
     * @param plog handle to current PLOG file to read
     */
    public void setPlog (PlogFile plog) {
        this.plog = plog;
    }

    /**
     * Return the PLOG file for which this reader is managing the reading and
     * converting of raw entries to domain records
     * 
     * @return the current open and valid PLOG file
     */
    public final PlogFile getPlog () {
        return this.plog;
    }

    /**
     * Set the pre-configured domain reader that takes care of reading
     * the stream of bytes as PLOG format records, prior to parsing and
     * converting it to domain records
     * 
     * @param domainReader the reader that holds domain specific knowledge
     *                     of the contents of a PLOG file and it's meaning
     */
    public void setDomainReader (DomainReader domainReader) {
        this.domainReader = domainReader;
    }

    /**
     * Return the domain reader that will be used to convert the byte stream
     * to domain records, either to configure it or the inherit its 
     * behavior, as is the case for proxies.
     * 
     * @return the domain reader to convert raw bytes to PLOG domain records
     */
    public DomainReader getDomainReader () {
        return this.domainReader;
    }

    /**
     * Set the size of domain record cache to keep before allowing it
     * to be flushed to caller
     * 
     * @param flushSize maximum size of cache before it will be flushed
     */
    public void setFlushSize (int flushSize) {
        this.flushSize = flushSize;
    }

    /**
     * Return the size of domain records to keep in cache prior to flushing
     * 
     * @return the size of cache at which it will be flushed
     */
    public int getFlushSize () {
        return this.flushSize;
    }

    /**
     * Return the internal record count of PLOG entry records read and 
     * converted to domain records.
     * 
     * @return count of records processed from PLOG stream so far
     */
    public int getRecordCount () {
        return this.recordCount;
    }

    /**
     * Set whether or not this PLOG stream reader is a proxy for a parent
     * PLOG stream, in effect a sub stream which will emit all of it's 
     * domain records at the offset of the record in the parent stream
     * that opened the proxy
     * 
     * @param isProxy true if the PLOG file is a load file and this is a proxy
     *                stream reader, else false
     */
    public void setIsProxy (boolean isProxy) {
        this.proxy = isProxy;
    }

    /**
     * Return whether or not this stream reader is a proxy reader for a
     * parent PLOG stream
     * 
     * @return true if a proxy stream reader, else false
     */
    public boolean isProxy () {
        return this.proxy;
    }

    /**
     * Set whether or not this stream reader is in a paused state, meaning it
     * will not be reading or advancing it's own data stream, but waiting
     * for a proxy or child stream to emit records in it's stead
     * 
     * @param paused true if paused and waiting for proxy to emit, else false
     */
    public void setPaused (boolean paused) {
        this.paused = paused;
    }

    /**
     * Retrun whether or not this PLOG stream reader has been paused and
     * waiting for proxy reader to emit records at it's paused offset
     * 
     * @return true if paused, else false
     */
    public boolean isPaused () {
        return paused;
    }

    /**
     * Open the PLOG stream reader at the beginning of the PLOG file
     * 
     * @throws Exception when it fails to open the PLOG stream at offset zero
     */
    public void open () throws Exception {
        open (0);
    }

    /**
     * Open a PLOG stream reader at a specific file offset. Firstly it opens
     * a buffered byte input stream on the physical PLOG file, reads the 
     * file header from stream (first 16 bytes) to verify that it's a valid
     * PLOG file to process, else it fails, next it reads the PLOG control
     * header entry record that determines the encoded feature set in PLOG,
     * lastly it forwards byte stream to given byte offset.
     * 
     * @param startOffset the byte offset at which to start reading records 
     *                    from
     *
     * @throws Exception when it fails to open the PLOG stream due to an 
     *                   unexpected error
     */
    public void open (long startOffset) throws Exception {
        try {
            this.plogStream = new DataInputStream (
                new BufferedInputStream (
                    new FileInputStream (new File (plog.getFullPath()))
                )
            );
            /* read header and position at first entry record */
            prepare ();

            if (startOffset > offset) {
                forward (startOffset);
            }
        }
        catch (EOFException e) {
            throw new Exception (
                "Failed to open stream reader for PLOG: " + 
                plog.getFileName() + " with incomplete header",
                e
            );
        }
        catch (Exception e) {
            String reason = null;
            
            if (e.getMessage() != null) {
                reason = ", reason: " + e.getMessage();
            }
            else {
                reason = "";
            }
            /* report a nice error first then let it pass on */
            throw new Exception (
                "Failed to open stream reader for PLOG: " + 
                plog.getFileName() + " at offset: "       + 
                startOffset + reason,
                e
            );
        }
    }

    /**
     * Use the domain reader to read one PLOG format entry record from PLOG
     * stream and parse it to one or more domain records, which if needed,
     * are persisted to domain cache of stream reader.
     * 
     * @return number of domain records persisted for the PLOG entry records
     *         read from stream so far, cumulative number of records
     * @throws Exception if any read or parse error occur
     */
    public int read () throws Exception {
        if (!ready || domainReader == null) {
            close();
            throw new Exception (
                "Unable to read from PLOG: " + plog.getFileName() + 
                ", reason: PLOG stream is invalid"
            );
        }

        if (!isDone()) {
            List<DomainRecord> drs = domainReader.read (this);

            if (drs != null) {
                for (DomainRecord dr : drs) {
                    if (dr.shouldPersist()) {
                        data.add (dr);
                    }

                    if (domainReader.isDone()) {
                        done = true;
                    }

                    /* total number of records parsed */
                    recordCount++;
                }
            }
        }

        return recordCount;
    }

    /**
     * Check whether or not the domain data cache can be flushed, either
     * the batch has reached flush size or the reader is done and last
     * batch should be emitted
     * 
     * @return true if the domain cache is ready for caller to take, else
     *         false, as in not ready, keep polling
     */
    public boolean canFlush() {
        return 
            data.size() > 0 &&
            (done || data.size() % flushSize == 0);
    }

    /**
     * Flushes the data cache of domain records, as in remove local references
     * to domain records and add it to outgoing list of records for client
     * to process and clear
     * 
     * @return list of domain records in batch
     */
    public List<DomainRecord> flush () {
        List<DomainRecord> clone = 
            new ArrayList<DomainRecord>(data);

        /* clear data list */
        data.clear();

        return clone;
    }

    /**
     * Closes the PLOG stream, reset internal state and clear out domain
     * cache
     * 
     * @throws Exception if it could not successfully close the stream reader
     */
    public void close () throws Exception {
        recordCount = 0;
        data.clear();
        resetOffset();
        
        if (plogStream != null) {
            plogStream.close();
        }
    }

    /**
     * Mark position in stream to be valid up to default read limit value,
     * retain physical offset marker for rewinding stream to
     * 
     * @return true if mark succeeded, else false, as in not supported
     */
    public boolean mark () {
        return mark (READ_LIMIT);
    }

    /**
     * Mark the current offset in stream to be valid up to provided limit
     * in bytes, this allows rewinding of stream to this position
     * 
     * @param limit number of bytes that can be read afterwards before this
     *              marker will be removed or invalidated, however we still
     *              keep a physical offset value for mark
     *
     * @return true if the stream supported the mark operation, else false
     *         if not supported
     */
    public boolean mark (int limit) {
        boolean marked = true;

        if (plogStream != null && plogStream.markSupported()) {
            plogStream.mark(limit);

            /* mark the current offset */
            marker = offset;
        }
        else {
            marked = false;
        }
        return marked;
    }

    /**
     * Rewind the stream to previous marked offset in PLOG stream and reset
     * the internal byte offset tracked as replicated object offset, to the
     * offset of previous marker
     * 
     * @throws Exception when PLOG stream failed to rewind or it is not
     *                   a supported action
     */
    public void rewind () throws Exception {
        if (plogStream != null && plogStream.markSupported()) {
            /* rewind/reset to previous mark */
            plogStream.reset();

            /* reset to marker */
            offset = marker;
        }
        else {
            throw new Exception (
                "PLOG stream did not support rewinding to the marked offset"
            );
        }
    }

    /**
     * Forward to the provided byte offset in the PLOG stream
     * 
     * @param newOffset the byte offset at which to start reading
     *
     * @throws Exception if it was unable to forward to correct offset
     *                   in stream or and I/O error occurred
     */
    public void forward (long newOffset) throws Exception  {
        if (newOffset < offset) {
            if (logger.isDebugEnabled()) {
                logger.debug (
                    "Not forwarding stream to offset " + newOffset + ", " +
                    "current offset is " + offset
                );
            }
            /* no need to forward */
            return;
        }

        /* actual bytes to skip from current offset */
        long skipBytes = newOffset - offset;
        /* track bytes skipped during skip loop */
        long skipped = 0L;

        if (plogStream != null) {
            /* skip loop for buffered byte stream */
            while (skipped != skipBytes) {
                long numBytes = plogStream.skip(skipBytes - skipped);
                skipped += numBytes;
            }

            /* set the stream offset */
            offset += skipped;

            if (offset != newOffset) {
                throw new Exception (
                    "Failed to skip to file offset: " + newOffset + " " +
                    "current offset: " + offset
                );
            }
        }
    }

    /**
     * Keep track of the internal byte offset within the PLOG stream
     * 
     * @param offset byte offset within PLOG stream of entry record read
     */
    public void setOffset (long offset) {
        this.offset = offset;
    }

    /**
     * Return the current byte offset at which the stream reader is
     * positioned to read next PLOG entry record
     * 
     * @return current byte offset in PLOG stream
     */
    public long getOffset () {
        return this.offset;
    }

    /**
     * Advance internal byte offset used to track the position within the
     * PLOG stream, unless this is a proxy stream reader then we emit
     * all records at the offset of it's parent stream. Using the PLOG 
     * identifier and the record offset within its stream is a fundamental
     * limitation
     * 
     * @param advance number of bytes to advance internal offset tracker
     */
    public void advanceOffset (long advance) {
        /* when acting as proxy or sub stream we do not advance offset and
         * emit all records at parent record's offset */
        if (!proxy) {
            this.offset += advance;
        }
    }

    /**
     * Reset the byte offset tracked as the position of the read in the 
     * PLOG stream
     */
    private void resetOffset () {
        this.offset = 0;
    }

    /**
     * Return the value of the offset where last stream marker was performed
     * 
     * @return byte offset where PLOG stream was marked for a rewind
     */
    public long getOffsetMarker() {
        return marker;
    }

    /** 
     * Check if PLOG stream is busy reading records, as in it has at least
     * read the PLOG control header
     *  
     * @return true if busy else false
     */
    public boolean isBusy () {
        boolean busy = false;
        if (offset >= PlogFile.PLOG_DATA_BYTE_OFFSET &&
            ready && 
            !done) 
        {
            busy = true;
        }

        return busy;
    }

    /**
     * Validate whether or not the PLOG stream is valid by reading the PLOG
     * file header and checking that it's the start of a PLOG file
     * 
     * @return true if a valid PLOG stream to process, else false
     * @throws Exception when an unexpected parse error occurred or the stream
     *                   is in an inconsistent state and it could not be
     *                   validated
     */
    public boolean isValid () throws Exception {
        boolean valid = false;

        if (offset == 0) {
            /* stream starts with header that specifies that it is a 
             * PLOG file */
            PlogHeader header = 
                PlogHeaderParser.getParser().parse(plogStream);
            advanceOffset (header.size());

            valid = header.isValid();
        }
        else {
            /* report error, this stream is not valid */
            throw new Exception (
                "Unable to validate stream header for PLOG: "  +
                plog.getFileName() + " at offset: " + offset   + 
                ", reason: header is always at offset 0, not " + 
                offset 
            );
        }

        return valid;
    }

    /**
     * Helper function for reading the set of features encoded in the PLOG
     * format, from the control header record. The feature set determines 
     * how to read or parse row-level change records
     * 
     * @throws Exception if any control header parse errors occurred
     */
    private void readPlogFeatures () throws Exception {
        /* always have to read the PLOG features when opening a PLOG */
        EntryRecord rec = EntryRecordParser.getParser().parse(plogStream);

        /* advance offset */
        advanceOffset(rec.getSize());

        if (rec.isHeader()) {
            HeaderParser headerParser = new HeaderParser();
            headerParser.parse (plog, rec);
            HeaderRecord hdr = (HeaderRecord) headerParser.emit();
            hdr.setRawRecordSize(rec.getSize());
            plog.setHeader(hdr);

            if (!hdr.isValid()) {
                throw new Exception ("Invalid PLOG file, unable to parse");
            }
        }
        else {
            throw new Exception (
                "Invalid PLOG stream, no header record found " +
                "after file header"
            );
        }
    }

}
