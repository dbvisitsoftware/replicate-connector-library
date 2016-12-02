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

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * Manages the configuration for a PLOG session
 */
public class PlogConfig {
    private static final Logger logger = LoggerFactory.getLogger(
        PlogConfig.class
    );
    
    /** Default configuration file */
    private final String DEFAULT_CONFIG_FILE = "replicate-config.properties";
    
    /** Lookup for configuration properties in PlogConfigType */
    private Map<PlogConfigType, String> config;
    
    /**
     * Initialise from property file
     * 
     * @param propFile property file on class path
     * 
     * @throws Exception for any configuration errors
     */
    private void init (String propFile) throws Exception {
        this.config = new HashMap<PlogConfigType, String>();
        
        /* read from file */
        InputStream input = null;
        Properties props = new Properties();
        try {
            input = 
                this.getClass().getClassLoader().getResourceAsStream(propFile);
            
            if(input == null) {
                logger.warn (
                    "Unable to read from property file: " + propFile + ", " +
                    "will use defaults"
                );
                for (PlogConfigType configType : PlogConfigType.values()) {
                    config.put (configType, configType.getDefaultValue());
                }
            }
            else {
                props.load(input);
            
                for (PlogConfigType configType : PlogConfigType.values()) {
                    String configValue = 
                        props.getProperty (configType.toString());
                    if (configValue != null) {
                        this.config.put (configType, configValue);
                    }
                    else if (
                        configValue == null && 
                        configType.getDefaultValue() != null
                    ) {
                        logger.debug (
                            "No property provided for " + 
                            configType.toString() +
                            ", using default of " + 
                            configType.getDefaultValue()
                        );
                        this.config.put (
                            configType,
                            configType.getDefaultValue()
                        );
                    }
                    else {
                        throw new Exception (
                            "No value provided for mandatory property: " +
                            configType.toString()
                        );
                    }
                }
            }
        } catch (Exception e) {
            logger.error (e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.debug ("Cause: ", e);
            }
            
            throw e;
        } finally {
            if(input != null) {
                try {
                    input.close();
                } catch (Exception e) {
                    logger.error (e.getMessage());
                    if (logger.isDebugEnabled()) {
                        logger.debug ("Cause: ", e);
                    }
                }
            }
        }
    }
    
    /**
     * Initialise from properties
     * 
     * @param props Map of properties to create configuration for
     * 
     * @throws Exception for any configuration errors
     */
    private void init (Map<String, String> props) throws Exception {
        this.config = new HashMap<PlogConfigType, String>();
        
        for (PlogConfigType configType : PlogConfigType.values()) {
            String key = configType.toString();
            
            if (props.containsKey(key)) {
                String configValue = props.get (key);
            
                this.config.put (configType, configValue);
            }
            else if (configType.getDefaultValue() != null)
            {
                logger.debug (
                    "No property provided for " + 
                    configType.toString() +
                    ", using default of " + 
                    configType.getDefaultValue()
                );
                this.config.put (
                    configType,
                    configType.getDefaultValue()
                );
            }
            else {
                throw new Exception (
                    "No value provided for mandatory property: " +
                    configType.toString()
                );
            }
        }
    }
    
    /** 
     * Create and initialises new PLOG configuration from default
     * config property file 
     * 
     * @throws Exception for any configuration errors 
     */
    public PlogConfig () throws Exception {
        init (DEFAULT_CONFIG_FILE);
    }
    
    /** 
     * Create and initialises new PLOG configuration from provided
     * config property file available on class path
     * 
     * @param propFile Property file to read configuration from
     * 
     * @throws Exception for any configuration errors 
     */
    public PlogConfig (String propFile) throws Exception {
        init (propFile);
    }
    
    /**
     * Create PLOG configuration from property map
     * 
     * @param props Property map to read configuration from
     * 
     * @throws Exception for any configuration errors
     */
    public PlogConfig (Map<String, String> props) throws Exception {
        init (props);
    }
    
    /**
     * Retrieve the value for a PLOG configuration property from internal
     * configuration lookup
     * 
     * @param configType PlogConfigType configuration type to search for
     * @return String value of configuration paramater as string
     * 
     * @throws Exception when no entry found for configuration type
     */
    public String getConfigValue (PlogConfigType configType) throws Exception {
        if (!config.containsKey (configType)) {
            throw new Exception (
                "Runtime error: unable to find configuration value for " +
                configType.toString()
            );
        }
        return config.get (configType);
    }
    
    /**
     * Set the value for a given PLOG configuration property
     * 
     * @param configType The PLOG configuration property type to set
     * @param value      The value, as string, for above property
     */
    public void setConfigValue (PlogConfigType configType, String value) {
        config.put (configType, value);
    }
    
    /**
     * Return the internal configuration as a simple property map of 
     * configuration parameter/value
     * 
     * @return property map of configuration parameter/value as strings
     */
    public Map<String, String> toPropertyMap () {
        Map<String, String> propMap = new HashMap<String, String>();
        
        for (PlogConfigType key : config.keySet()) {
            propMap.put (key.toString(), config.get(key));
        }
        
        return propMap;
    }
    
}
