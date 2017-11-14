/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package  org.apache.nifi.processors.payoneer;

import com.jayway.jsonpath.JsonPath;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hbase.HBaseClientService;
import org.apache.nifi.hbase.increment.IncrementColumn;
import org.apache.nifi.hbase.increment.IncrementColumnResult;
import org.apache.nifi.hbase.increment.IncrementFlowFile;
import org.apache.nifi.hbase.put.PutColumn;
import org.apache.nifi.hbase.put.PutFlowFile;
import org.apache.nifi.json.JsonPathValidator;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"hadoop", "hbase","lock"})

abstract class AbstractHBaseMultipleLockProcessor extends AbstractProcessor {
    static final String STRING_ENCODING_VALUE = "String";
    static final String BINARY_ENCODING_VALUE = "Binary";

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be sent to HBase")
            .build();
    protected static final AllowableValue ROW_ID_ENCODING_STRING = new AllowableValue(STRING_ENCODING_VALUE, STRING_ENCODING_VALUE,
            "Stores the value of row id as a UTF-8 String.");
    protected static final AllowableValue ROW_ID_ENCODING_BINARY = new AllowableValue(BINARY_ENCODING_VALUE, BINARY_ENCODING_VALUE,
            "Stores the value of the rows id as a binary byte array. It expects that the row id is a binary formatted string.");


    static final PropertyDescriptor ROW_ID_ENCODING_STRATEGY = new PropertyDescriptor.Builder()
            .name("Row Identifier Encoding Strategy")
            .description("Specifies the data type of Row ID used when inserting data into HBase. The default behavior is" +
                    " to convert the row id to a UTF-8 byte array. Choosing Binary will convert a binary formatted string" +
                    " to the correct byte[] representation. The Binary option should be used if you are using Binary row" +
                    " keys in HBase")
            .required(false) // not all sub-classes will require this
            .expressionLanguageSupported(false)
            .defaultValue(ROW_ID_ENCODING_STRING.getValue())
            .allowableValues(ROW_ID_ENCODING_STRING,ROW_ID_ENCODING_BINARY)
            .build();

    protected static final PropertyDescriptor HBASE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("HBase Client Service")
            .description("Specifies the Controller Service to use for accessing HBase.")
            .required(true)
            .identifiesControllerService(HBaseClientService.class)
            .build();
    protected static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the HBase Table to put data into")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor COLUMN_FAMILY = new PropertyDescriptor.Builder()
            .name("Column Family")
            .description("The Column Family to use when inserting data into HBase")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor COLUMN_QUALIFIER = new PropertyDescriptor.Builder()
            .name("Column Qualifier")
            .description("The Column Qualifier to use when inserting data into HBase")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    protected static final PropertyDescriptor TIMESTAMP = new PropertyDescriptor.Builder()
            .name("timestamp")
            .displayName("Timestamp")
            .description("The timestamp for the job start")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .required(true)
            .defaultValue("${now():toNumber()}")
            .build();

    protected static final PropertyDescriptor JSON_PATH = new PropertyDescriptor.Builder()
             .name("Id JSON path")
            .description("JsonPath Expression that indicates how to retrieve the Id")
            .required(true)
            .addValidator(new JsonPathValidator())
            .build();


    protected static final PropertyDescriptor LOCK_ID = new PropertyDescriptor.Builder()
            .name("Lock Id")
            .description("The Id to lock for")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();



    protected HBaseClientService clientService;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        clientService = context.getProperty(HBASE_CLIENT_SERVICE).asControllerService(HBaseClientService.class);
    }


    protected byte[] getRow(final String row, final String encoding) {
        //check to see if we need to modify the rowKey before we pass it down to the PutFlowFile
        byte[] rowKeyBytes = null;
        if(BINARY_ENCODING_VALUE.contentEquals(encoding)){
            rowKeyBytes = clientService.toBytesBinary(row);
        }else{
            rowKeyBytes = row.getBytes(StandardCharsets.UTF_8);
        }
        return rowKeyBytes;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String columnFamily = context.getProperty(COLUMN_FAMILY).evaluateAttributeExpressions(flowFile).getValue();
        final String columnQualifier = context.getProperty(COLUMN_QUALIFIER).evaluateAttributeExpressions(flowFile).getValue();
        final String lockId = context.getProperty(LOCK_ID).evaluateAttributeExpressions(flowFile).getValue();
        final String rowIdEncodingStrategy = context.getProperty(ROW_ID_ENCODING_STRATEGY).getValue();

        final String jsonPath = context.getProperty(JSON_PATH).evaluateAttributeExpressions(flowFile).getValue();

        final String timestampValue = context.getProperty(TIMESTAMP).evaluateAttributeExpressions(flowFile).getValue();
        final byte[] lockId_bytes = lockId.getBytes(StandardCharsets.UTF_8);


        final Long timestamp;
        if (!StringUtils.isBlank(timestampValue)) {
            try {
                timestamp = Long.valueOf(timestampValue);
            } catch (Exception e) {
                getLogger().error("Invalid timestamp value: " + timestampValue, e);
                throw new ProcessException(e);
            }
        } else {
            timestamp = new Date().getTime();
        }
        final List<String> lock_ids = new ArrayList<>();
        session.read(flowFile, in -> {
            try (final InputStream bufferedIn = new BufferedInputStream(in)) {
                 List<String> locks = JsonPath.read(bufferedIn,jsonPath);
                 lock_ids.addAll(locks);
            }
        });

        getLogger().info("Got {} locks ({})",new Object[]{lock_ids.size(),lock_ids});
try {
    handleLock(session, flowFile, tableName, columnFamily, columnQualifier, lockId, rowIdEncodingStrategy, lockId_bytes, timestamp, lock_ids);
}catch (Exception ex)
{
    getLogger().error("Could not Acquire lock", ex);
    session.transfer(flowFile,REL_FAILURE);
}

    }

    protected abstract void handleLock(ProcessSession session, FlowFile flowFile, String tableName, String columnFamily, String columnQualifier, String lockId, String rowIdEncodingStrategy, byte[] lockId_bytes, Long timestamp, List<String> lock_ids) ;

}
