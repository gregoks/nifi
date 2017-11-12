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
package org.apache.nifi.hbase;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hbase.delete.DeleteColumn;
import org.apache.nifi.hbase.delete.DeleteFlowFile;
import org.apache.nifi.hbase.put.PutFlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

@EventDriven
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"hadoop", "hbase"})
@CapabilityDescription("Adds the attribute values of a FlowFile to HBase as the value of a single cell")
public class DeleteHBaseAttribute extends AbstractWriteHBase {
    protected static final PropertyDescriptor DELETE_COLUMNS = new PropertyDescriptor.Builder()
            .name("Delete cells")
            .description("A comma seperated list of cells to remove if the test is successfull")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^[a-zA-Z]\\w*:[a-zA-Z]\\w*(,[a-zA-Z]\\w*:[a-zA-Z]\\w*)*$")))
            .build();

    protected static final PropertyDescriptor TEST_VALUE = new PropertyDescriptor.Builder()
            .name("Test Value")
            .description("The value to test if exists in the cell before deleting")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_NOMATCH = new Relationship.Builder()
            .name("no match")
            .description("A FlowFile is routed to this relationship if the row was not matched")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HBASE_CLIENT_SERVICE);
        properties.add(TABLE_NAME);
        properties.add(ROW_ID);
        properties.add(ROW_ID_ENCODING_STRATEGY);
        properties.add(COLUMN_FAMILY);
        properties.add(COLUMN_QUALIFIER);
        properties.add(TEST_VALUE);
        properties.add(DELETE_COLUMNS);

        properties.add(BATCH_SIZE);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        rels.add(REL_NOMATCH);
        return rels;
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles == null || flowFiles.size() == 0) {
            return;
        }
        final Map<String,List<DeleteFlowFile>> tableDeletes = new HashMap<>();
        // Group FlowFiles by HBase Table
        for (final FlowFile flowFile : flowFiles) {
            final DeleteFlowFile deleteFlowFile = createDelete(session, context, flowFile);

            if (deleteFlowFile == null) {
                // sub-classes should log appropriate error messages before returning null
                session.transfer(flowFile, REL_FAILURE);
            } else if (!deleteFlowFile.isValid()) {
                if (org.apache.commons.lang3.StringUtils.isBlank(deleteFlowFile.getTableName())) {
                    getLogger().error("Missing table name for FlowFile {}; routing to failure", new Object[]{flowFile});
                } else if (null == deleteFlowFile.getRow()) {
                    getLogger().error("Missing row id for FlowFile {}; routing to failure", new Object[]{flowFile});
                } else if (deleteFlowFile.getColumns() == null || deleteFlowFile.getColumns().isEmpty()) {
                    getLogger().error("No columns provided for FlowFile {}; routing to failure", new Object[]{flowFile});
                } else {
                    // really shouldn't get here, but just in case
                    getLogger().error("Failed to produce a delete for FlowFile {}; routing to failure", new Object[]{flowFile});
                }
                session.transfer(flowFile, REL_FAILURE);
            } else {
                List<DeleteFlowFile> deleteFlowFiles = tableDeletes.get(deleteFlowFile.getTableName());
                if (deleteFlowFiles == null) {
                    deleteFlowFiles = new ArrayList<>();
                    tableDeletes.put(deleteFlowFile.getTableName(), deleteFlowFiles);
                }
                deleteFlowFiles.add(deleteFlowFile);
            }
        }

        getLogger().debug("Sending {} FlowFiles to HBase in {} deletes operations", new Object[]{flowFiles.size(), tableDeletes.size()});

        final long start = System.nanoTime();
        final List<DeleteFlowFile> successes = new ArrayList<>();
        final List<DeleteFlowFile> failures = new ArrayList<>();

        for (Map.Entry<String, List<DeleteFlowFile>> entry : tableDeletes.entrySet()) {
            String table = entry.getKey();
            for(DeleteFlowFile deleteFlowFile: entry.getValue()) {
                try {
                    if(clientService.checkAndDelete(table,deleteFlowFile.getRow(),deleteFlowFile.getFamily(),deleteFlowFile.getQualifier(),deleteFlowFile.getValue(),deleteFlowFile.getColumns())){
                        successes.add(deleteFlowFile);
                    }else{
                        failures.add(deleteFlowFile);
                    }


                } catch (Exception e) {
                    getLogger().error(e.getMessage(), e);
                    getLogger().error("Failed to send {} to HBase due to {}; routing to failure", new Object[]{deleteFlowFile.getFlowFile(), e});
                    final FlowFile failure = session.penalize(deleteFlowFile.getFlowFile());
                    session.transfer(failure, REL_FAILURE);
                }
            }
        }

        final long sendMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        getLogger().debug("Sent {} FlowFiles to HBase successfully in {} milliseconds", new Object[]{successes.size(), sendMillis});

        for (DeleteFlowFile deleteFlowFile : successes) {
            session.transfer(deleteFlowFile.getFlowFile(), REL_SUCCESS);
            final String details = "Deleted " + deleteFlowFile.getColumns().size() + " cells to HBase";
            session.getProvenanceReporter().send(deleteFlowFile.getFlowFile(), getTransitUri(deleteFlowFile), details, sendMillis);
        }

        for (DeleteFlowFile deleteFlowFile : failures) {
            session.transfer(deleteFlowFile.getFlowFile(), REL_NOMATCH);
            final String details = "No match for delete";
            session.getProvenanceReporter().send(deleteFlowFile.getFlowFile(), getTransitUri(deleteFlowFile), details, sendMillis);
        }

    }


    protected String getTransitUri(AbstractActionFlowFile actionFlowFile) {
        return "hbase://" + actionFlowFile.getTableName() + "/" + new String(actionFlowFile.getRow(), StandardCharsets.UTF_8);
    }

    protected DeleteFlowFile createDelete(final ProcessSession session, final ProcessContext context, final FlowFile flowFile) {
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String row = context.getProperty(ROW_ID).evaluateAttributeExpressions(flowFile).getValue();
        final String columnFamily = context.getProperty(COLUMN_FAMILY).evaluateAttributeExpressions(flowFile).getValue();
        final String columnQualifier = context.getProperty(COLUMN_QUALIFIER).evaluateAttributeExpressions(flowFile).getValue();
        final String timestampValue = context.getProperty(TIMESTAMP).evaluateAttributeExpressions(flowFile).getValue();
        final String value = context.getProperty(TEST_VALUE).evaluateAttributeExpressions(flowFile).getValue();


        final Long timestamp;
        if (!StringUtils.isBlank(timestampValue)) {
            try {
                timestamp = Long.valueOf(timestampValue);
            } catch (Exception e) {
                getLogger().error("Invalid timestamp value: " + timestampValue, e);
                return null;
            }
        } else {
            timestamp = null;
        }

        Collection<DeleteColumn> columns = new ArrayList<>();

        for(Map.Entry<PropertyDescriptor,String> entry : context.getProperties().entrySet()){
            if(!entry.getKey().isDynamic()) continue;
            String fieldName = entry.getKey().getName();
            int split = fieldName.indexOf(':');
            if(split <=0)
            {
                getLogger().error("Entry {} is not a valid cell name (family:qualifier)",new Object[]{columnFamily});
                return null;

            }
            String cFamily = fieldName.substring(0,split);
            String cQualifier = fieldName.substring(split+1);


            DeleteColumn column = new DeleteColumn(cFamily.getBytes(StandardCharsets.UTF_8),
                    cQualifier.getBytes(StandardCharsets.UTF_8),timestamp);
            columns.add(column);
        }


        byte[] rowKeyBytes = getRow(row,context.getProperty(ROW_ID_ENCODING_STRATEGY).getValue());
        byte[] colFamily = columnFamily.getBytes(StandardCharsets.UTF_8);
        byte[] colQualifier = columnQualifier.getBytes(StandardCharsets.UTF_8);
        byte[] colValue = null;
        if(value != null)
            colValue = value.getBytes(StandardCharsets.UTF_8);

        return new DeleteFlowFile(tableName,rowKeyBytes , colFamily, colQualifier, colValue, columns, flowFile);
    }



}