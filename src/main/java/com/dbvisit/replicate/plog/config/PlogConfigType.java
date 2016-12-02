package com.dbvisit.replicate.plog.config;

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

/**
 * Define configuration properties for PLOG processing.
 */
public enum PlogConfigType {
    /** The file URI of MINE's PLOGs */
    PLOG_LOCATION_URI ("plog.location.uri", null),
    /** Flush size of data cache */
    DATA_FLUSH_SIZE ("plog.data.flush.size", "1000"),
    /** Wait time in milliseconds for scan interval */
    SCAN_WAIT_TIME_MS ("plog.interval.time.ms", "500"),
    /** Number of wait intervals between scans */
    SCAN_INTERVAL_COUNT ("plog.scan.interval.count", "5"),
    /** Number of scan interval considered as health check */ 
    HEALTH_CHECK_INTERVAL_COUNT ("plog.health.check.interval", "10"),
    /** Number of health check scans prior to considering it offline */
    SCAN_QUIT_INTERVAL_COUNT ("plog.scan.offline.interval", "100");
    
    private final String property;
    private final String defaultValue;
    
    /**
     * Create a new PLOG configuration property/type from String parameter
     * and value
     * 
     * @param param The configuration property string
     * @param value The value, as string, for configuration property
     */
    PlogConfigType (String param, String value) {
        this.property = param;
        this.defaultValue = value;
    }
    
    /**
     * Return the PLOG configuration type's property string
     * 
     * @return property string
     */
    public String getProperty() {
        return this.property;
    }
    
    /**
     * Return the value for a PLOG configuration type
     * 
     * @return value for configuration, as string
     */
    public String getDefaultValue() {
        return this.defaultValue;
    }

    /** Use configuration parameter as string representation 
     * 
     * @return configuration property string
     */
    @Override
    public String toString() {
        return getProperty();
    }
    
    /** 
     * Linear search on configuration parameter 
     * 
     * @param property The property string to lookup PlogConfigType for
     * 
     * @return PlogConfigType return the configuration type for property
     *         string or null if none found
     */
    public static PlogConfigType find(String property) {
        for (PlogConfigType type : PlogConfigType.values()) {
            if (type.getProperty().equalsIgnoreCase(property)) {
                return type;
            }
        }
        return null;
    }
    
}
