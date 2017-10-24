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
import org.apache.nifi.hbase.increment.IncrementColumn;
import org.apache.nifi.hbase.increment.IncrementColumnResult;
import org.apache.nifi.hbase.increment.IncrementFlowFile;
import org.apache.nifi.hbase.put.PutColumn;
import org.apache.nifi.hbase.put.PutFlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processor.util.StandardValidators.LONG_VALIDATOR;

@EventDriven
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"hadoop", "hbase"})
@CapabilityDescription("Increments HBase cells based on attributes of a FlowFile")
public class IncrementHBaseCells extends AbstractWriteHBase {

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HBASE_CLIENT_SERVICE);
        properties.add(TABLE_NAME);
        properties.add(ROW_ID);
        properties.add(ROW_ID_ENCODING_STRATEGY);
        properties.add(COLUMN_FAMILY);
        properties.add(BATCH_SIZE);
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("Column name Expression that will be incremented by the extracted delta")
                .dynamic(true)
                .required(false)
                .expressionLanguageSupported(true)
                .addValidator(LONG_VALIDATOR)
                .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        return rels;
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles == null || flowFiles.size() == 0) {
            return;
        }

        final Map<String,List<IncrementFlowFile>> tableIncrements = new HashMap<>();

        // Group FlowFiles by HBase Table
        for (final FlowFile flowFile : flowFiles) {
            final IncrementFlowFile incrementFlowFile = createIncrement(session, context, flowFile);

            if (incrementFlowFile == null) {
                // sub-classes should log appropriate error messages before returning null
                session.transfer(flowFile, REL_FAILURE);
            } else if (!incrementFlowFile.isValid()) {
                if (org.apache.commons.lang3.StringUtils.isBlank(incrementFlowFile.getTableName())) {
                    getLogger().error("Missing table name for FlowFile {}; routing to failure", new Object[]{flowFile});
                } else if (null == incrementFlowFile.getRow()) {
                    getLogger().error("Missing row id for FlowFile {}; routing to failure", new Object[]{flowFile});
                } else if (incrementFlowFile.getColumns() == null || incrementFlowFile.getColumns().isEmpty()) {
                    getLogger().error("No columns provided for FlowFile {}; routing to failure", new Object[]{flowFile});
                } else {
                    // really shouldn't get here, but just in case
                    getLogger().error("Failed to produce a put for FlowFile {}; routing to failure", new Object[]{flowFile});
                }
                session.transfer(flowFile, REL_FAILURE);
            } else {
                List<IncrementFlowFile> incFlowFiles = tableIncrements.get(incrementFlowFile.getTableName());
                if (incFlowFiles == null) {
                    incFlowFiles = new ArrayList<>();
                    tableIncrements.put(incrementFlowFile.getTableName(), incFlowFiles);
                }
                incFlowFiles.add(incrementFlowFile);
            }
        }

        getLogger().debug("Sending {} FlowFiles to HBase in {} increment operations", new Object[]{flowFiles.size(), tableIncrements.size()});

        final long start = System.nanoTime();
        final List<IncrementFlowFile> successes = new ArrayList<>();

        for (Map.Entry<String, List<IncrementFlowFile>> entry : tableIncrements.entrySet()) {
            try {
                clientService.increment(entry.getKey(), entry.getValue());
                successes.addAll(entry.getValue());
            } catch (Exception e) {
                getLogger().error(e.getMessage(), e);

                for (IncrementFlowFile incrementFlowFile : entry.getValue()) {
                    getLogger().error("Failed to send {} to HBase due to {}; routing to failure", new Object[]{incrementFlowFile.getFlowFile(), e});
                    final FlowFile failure = session.penalize(incrementFlowFile.getFlowFile());
                    session.transfer(failure, REL_FAILURE);
                }
            }
        }

        final long sendMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        getLogger().debug("Sent {} FlowFiles to HBase successfully in {} milliseconds", new Object[]{successes.size(), sendMillis});

        for (IncrementFlowFile incrementFlowFile : successes) {
            StringBuilder sb = new StringBuilder("{");
            for(IncrementColumnResult col : incrementFlowFile.getColumnResults()){

                if(col.getValue() != null) {
                     //if we have a value, add it as an attribute
                    session.putAttribute(incrementFlowFile.getFlowFile(),
                            "hbase.increment." + new String(col.getColumnFamily(), StandardCharsets.UTF_8)
                                    + "." + new String(col.getColumnQualifier(), StandardCharsets.UTF_8),
                            col.getValue().toString());
                }

            }


            session.transfer(incrementFlowFile.getFlowFile(), REL_SUCCESS);
            final String details = "Put " + incrementFlowFile.getColumns().size() + " cells to HBase";
            session.getProvenanceReporter().send(incrementFlowFile.getFlowFile(), getTransitUri(incrementFlowFile), details, sendMillis);
        }

    }

    protected IncrementFlowFile createIncrement(final ProcessSession session, final ProcessContext context, final FlowFile flowFile) {
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String row = context.getProperty(ROW_ID).evaluateAttributeExpressions(flowFile).getValue();
        final String columnFamily = context.getProperty(COLUMN_FAMILY).evaluateAttributeExpressions(flowFile).getValue();
        //final String columnQualifier = context.getProperty(COLUMN_QUALIFIER).evaluateAttributeExpressions(flowFile).getValue();


        List<IncrementColumn> columns = new ArrayList<>();

        for(Map.Entry<PropertyDescriptor,String> entry : context.getProperties().entrySet()){
            if(!entry.getKey().isDynamic()) continue;
            String columnQualifier = entry.getKey().getName();
            Long delta = context.getProperty(entry.getKey().getName()).evaluateAttributeExpressions(flowFile).asLong();
            if (delta == null) {
                getLogger().error("Invalid Delta value for: "+ columnQualifier);
                return null;
            }
            IncrementColumn incrementColumn = new IncrementColumn(columnFamily.getBytes(StandardCharsets.UTF_8),
                    columnQualifier.getBytes(StandardCharsets.UTF_8),delta);
            columns.add(incrementColumn);
        }
        //return columns;

        byte[] rowKeyBytes = getRow(row,context.getProperty(ROW_ID_ENCODING_STRATEGY).getValue());


        return new IncrementFlowFile(tableName,rowKeyBytes , columns, flowFile);
    }



}
