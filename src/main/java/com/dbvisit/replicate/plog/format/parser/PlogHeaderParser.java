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

import com.dbvisit.replicate.plog.format.PlogHeader;

/**
 * Parses the PLOG file header from the start of an input data stream 
 * opened on what may be a PLOG file
 */
public class PlogHeaderParser extends Parser implements IParser {
    /** Use as singleton, it has no state */
    private PlogHeaderParser() {}
    /** The instance of PLOG file header parser */
    private static PlogHeaderParser INSTANCE = new PlogHeaderParser();

    /**
     * Return the parser to use for parsing raw PLOG file header
     * 
     * @return PLOG file header parser
     */
    public static PlogHeaderParser getParser() {
        return INSTANCE;
    }

    /**
     * Parse the PLOG file header at the beginning of a byte input stream,
     * this file header identifies a stream as that belonging to a 
     * valid PLOG file. The file header consist of:
     * 
     * <pre>
     * 8 bytes - PLOG file signature
     * 4 bytes - PLOG file format major version number
     * 4 bytes - PLOG file format minor version number
     * </pre>
     * 
     * @param input byte input stream opened at start of PLOG file
     * 
     * @return PLOG file header object
     * @throws Exception when unable to parse file header
     */
    public PlogHeader parse(final DataInputStream input)
    throws Exception {
        PlogHeader header  = new PlogHeader();

        byte[] signatureBytes = new byte[PlogHeader.HEADER_SIGNATURE_LENGTH];

        for (int i = 0; i < PlogHeader.HEADER_SIGNATURE_LENGTH; i++) {
            signatureBytes[i] = input.readByte();
        }

        header.setSignature (signatureBytes);
        header.setMajorVersion (parseSwappedInteger(input));
        header.setMinorVersion (parseSwappedInteger(input));

        return header;
    }

}
