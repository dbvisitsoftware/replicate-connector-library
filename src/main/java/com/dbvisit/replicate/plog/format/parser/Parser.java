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
import java.io.IOException;

/** 
 * Abstract parser that implements behavior for all raw format parsers to use
 */
public abstract class Parser {
    @SuppressWarnings("serial")
    public class StreamClosedException extends Exception {}

    /**
     * Reads an PLOG integer value from a PLOG InputStream. The value is 
     * converted to the opposed endian system while reading.
     * 
     * @param  input Source InputStream
     * 
     * @return       Parsed 4 byte integer value
     * @throws       Exception in case of an I/O problem
     */
    public int parseSwappedInteger(final DataInputStream input)
    throws Exception
    {
        if (input == null) {
            throw new StreamClosedException ();
        }

        byte[] bytes = new byte[4];
        
        try {
            for (int i = 0; i < 4; i++) {
                bytes[i] = input.readByte();
            }
        }
        catch (IOException e) {
            String err = e.getMessage();
            if (err != null && err.equals ("Stream closed")) {
                /* stream closed by parent stream reader */
                throw new StreamClosedException ();
            }
            else {
                throw e;
            }
        }

        return convertEndian (bytes);
    }

    /**
     * Converts a 4 bytes to integer of opposed endianness
     * 
     * @param bytes Input integer as 4 bytes
     * 
     * @return      Integer value in opposed endianness   
     */
    private int convertEndian (byte[] bytes) {
        assert (bytes.length == 4);

        return
            ((bytes[0] & 0xFF) << 0) 
                + 
            ((bytes[1] & 0xFF) << 8)
                + 
            ((bytes[2] & 0xFF) << 16) 
                + 
            ((bytes[3] & 0xFF) << 24
        );
    } 

}
