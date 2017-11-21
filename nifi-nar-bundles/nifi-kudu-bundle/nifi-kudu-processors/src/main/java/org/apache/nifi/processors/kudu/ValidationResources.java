package org.apache.nifi.processors.kudu;


import org.apache.hadoop.conf.Configuration;

/**
 * A helper class for maintaining loaded configurations (to avoid reloading on use unless necessary)
 */
public class ValidationResources {

    private final String configResources;
    private final Configuration configuration;

    public ValidationResources(String configResources, Configuration configuration) {
        this.configResources = configResources;
        this.configuration = configuration;
    }

    public String getConfigResources() {
        return configResources;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}