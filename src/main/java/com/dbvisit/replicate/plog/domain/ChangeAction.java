package com.dbvisit.replicate.plog.domain;

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
import java.util.Map;

import com.dbvisit.replicate.plog.format.EntrySubType;

/**
 * The supported change actions that can be applied for a logical change.
 */
public enum ChangeAction {
    /** New record inserted */
    INSERT,
    /** Existing record updated */
    UPDATE,
    /** Existing record removed */
    DELETE,
    /** New large object record inserted or existing one updated */
    LOB_WRITE,
    /** Existing large object record removed */
    LOB_ERASE,
    /** Existing large object record trim */
    LOB_TRIM,
    /** A change to record's data definition */
    DDL_OPERATION,
    /** No operation, used for record dictionary */
    NO_OPERATION,
    /** No change */
    NONE;
    
    /**
     * Lookup table for converting raw PLOG entry record types to domain
     * change action types
     */
    @SuppressWarnings("serial")
    private static Map<EntrySubType, ChangeAction> actionLookup =
        new HashMap<EntrySubType, ChangeAction> () {{
            put (EntrySubType.ESTYPE_LCR_INSERT, INSERT);
            put (EntrySubType.ESTYPE_LCR_UPDATE, UPDATE);
            put (EntrySubType.ESTYPE_LCR_DELETE, DELETE);
            put (EntrySubType.ESTYPE_LCR_LOB_WRITE, LOB_WRITE);
            put (EntrySubType.ESTYPE_LCR_LOB_ERASE, LOB_ERASE);
            put (EntrySubType.ESTYPE_LCR_LOB_TRIM, LOB_TRIM);
            put (EntrySubType.ESTYPE_LCR_DDL, DDL_OPERATION);
            put (EntrySubType.ESTYPE_LCR_NOOP, NO_OPERATION);
        }};
        
    /**
     * Lookup a change action by entry sub type
     * 
     * @param type Entry sub type to use in search for change action
     * @return     The change action associated with entry sub type provided
     */
    public static ChangeAction find (EntrySubType type) {
        ChangeAction action = NONE;
        
        if (actionLookup.containsKey (type)) {
            action = actionLookup.get (type);
        }
        
        return action;
    }
    
    /**
     * String representation of the change action enum name
     * 
     * @return string representation of a change action
     */
    @Override
    public String toString () {
        return this.name().replaceAll ("_", " ");
    }
    
}
