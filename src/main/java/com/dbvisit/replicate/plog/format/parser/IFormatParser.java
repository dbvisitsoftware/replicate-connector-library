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

/**
 * Parser interface, defines behavior that all parsers need to adhere to.
 */
public interface IFormatParser {
    /**
     * Parse behavior for all raw format parsers, these are parsers that
     * are reading raw format objects from a PLOG data input stream
     * 
     * @param input byte input stream opened on PLOG file
     * 
     * @return format object parsed
     * @throws Exception throw exception when any parser error occur
     */
    public Object parse(final DataInputStream input) throws Exception;
}
