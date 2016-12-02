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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PLOG file header data
 */
public class PlogHeader {
    private static final Logger logger = LoggerFactory.getLogger(
        PlogHeader.class
    );

    /** Length of PLOG header in bytes */
    private static final int HEADER_BYTES_LENGTH     = 16;
    /** Length of PLOG signature string in header */
    public static final int HEADER_SIGNATURE_LENGTH  = 8;
    /** The major version of the PLOG file format supported */
    private static final int PLOG_MAJOR_VERSION      = 1;
    /** The minor version of the PLOG file format supported */
    private static final int PLOG_MINOR_VERSION      = 1;
    /** The character signature string to identify a PLOG file */
    private static final String PLOG_SIGNATURE          = "PLOG\n \r ";
    /** The default encoding of the signature string is US ASCII */
    private static final String PLOG_SIGNATURE_CHAR_ENC = "US-ASCII";
    /** The raw signature of PLOG, as bytes */
    private byte[] signature = new byte[HEADER_SIGNATURE_LENGTH];
    /** The decoded major version of the current PLOG file */
    private int majorVersion;
    /** The decoded minor version of the current PLOG file */
    private int minorVersion;

    /**
     * Set the raw signature decoded from PLOG as bytes
     * 
     * @param signature bytes decoded from PLOG file header
     * 
     * @throws Exception if the number of bytes decoded do not match expected
     *                   number
     */
    public void setSignature (byte [] signature) throws Exception {
        if (signature.length != HEADER_SIGNATURE_LENGTH) {
            throw new Exception (
                "Invalid PLOG signature provided " + signature.toString() +
                ", reason: length is not " + HEADER_SIGNATURE_LENGTH
            );
        }
        this.signature = signature;
    }

    /**
     * Return the signature of PLOG file, as decoded bytes
     * 
     * @return raw signatures, as bytes
     */
    public byte[] getSignature() {
        return this.signature;
    }

    /**
     * Set the major version number, as decoded from 3rd chunk in PLOG file
     * header
     * 
     * @param majorVersion the major version number
     */
    public void setMajorVersion (int majorVersion) {
        this.majorVersion = majorVersion;
    }

    /**
     * Return the major version number of the PLOG file being parsed
     * 
     * @return the major version number of the PLOG file
     */
    public int getMajorVersion () {
        return this.majorVersion;
    }

    /**
     * Set the minor version number, as decoded from 4th chunk in PLOG file
     * header
     * 
     * @param minorVersion the minor version number
     */
    public void setMinorVersion (int minorVersion) {
        this.minorVersion = minorVersion;
    }

    /**
     * Return the minor version number of the PLOG file being parsed
     * 
     * @return the minor version number of the PLOG file
     */
    public int getMinorVersion () {
        return this.minorVersion;
    }

    /**
     * Converts the raw decoded signature bytes as a String
     * 
     * @return The human readable signature string
     * @throws Exception for any character set conversion errors
     */
    private String signatureAsString() throws Exception {
        String signatureStr = null;

        try {
            /* the expected value is 7-bit ASCII, so no charset magic is 
             * needed here
             */
            signatureStr = new String (this.signature, PLOG_SIGNATURE_CHAR_ENC);
        }
        catch (Exception e) {
            throw new Exception (
                "Invalid character set conversion: " + e.getMessage()
            );
        }

        return signatureStr;
    }

    /**
     * Determine if the PLOG file header is valid by checking if the
     * decoded signature string matches the expected PLOG marker and
     * the major/minor version numbers match the supported version
     * number
     * 
     * @return true if this is a valid PLOG file header, and the PLOG
     *         can be parsed, else false
     */
    public boolean isValid() {
        boolean valid = true;
        try {
            valid = signatureAsString().equals (PLOG_SIGNATURE);

            if (!valid) {
                logger.error ("File header does not denote a PLOG file.");
            }
        }
        catch (Exception e) {
            logger.error (e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.debug ("Cause: ", e);
            }
            valid = false;
        }

        if (this.majorVersion != PLOG_MAJOR_VERSION ||
            this.minorVersion != PLOG_MINOR_VERSION) {
            valid = false;

            logger.error (
                "Invalid file header version " + this.majorVersion + "." +
                this.minorVersion  + ", reason: expect version " +
                PLOG_MAJOR_VERSION + "." + PLOG_MINOR_VERSION       
            );
        }

        return valid;
    }

    /**
     * Return the fixed size of the PLOG file header in bytes, needed
     * for tracking stream offset as bytes are parsed from PLOG data
     * stream
     * 
     * @return size in bytes, as defined for PLOG file header
     */
    public int size() {
        return HEADER_BYTES_LENGTH;
    }

}
