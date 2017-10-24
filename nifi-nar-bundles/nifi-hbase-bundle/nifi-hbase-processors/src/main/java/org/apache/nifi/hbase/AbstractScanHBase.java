package org.apache.nifi.hbase;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hbase.scan.Column;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Pattern;

public abstract class AbstractScanHBase extends AbstractProcessor {

    static final Pattern COLUMNS_PATTERN = Pattern.compile("\\w+(:[\\w$]+)?(?:,\\w+(:[\\w$]+)?)*");


    static final PropertyDescriptor HBASE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("HBase Client Service")
            .description("Specifies the Controller Service to use for accessing HBase.")
            .required(true)
            .identifiesControllerService(HBaseClientService.class)
            .build();

    static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("Specifies which character set is used to encode the data in HBase")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();


    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles are routed to this relationship")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All failed fetches are routed to this relationship.")
            .build();


    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }




    // present for tests
    protected int getBatchSize() {
        return 500;
    }
}
