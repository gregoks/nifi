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
import org.apache.nifi.hbase.delete.DeleteColumn;
import org.apache.nifi.hbase.increment.IncrementColumn;
import org.apache.nifi.hbase.increment.IncrementColumnResult;
import org.apache.nifi.hbase.increment.IncrementFlowFile;
import org.apache.nifi.hbase.put.PutColumn;
import org.apache.nifi.hbase.put.PutFlowFile;
import org.apache.nifi.hbase.scan.Column;
import org.apache.nifi.hbase.scan.ResultCell;
import org.apache.nifi.hbase.scan.ResultHandler;
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
import java.util.concurrent.atomic.AtomicInteger;

@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"hadoop", "hbase","unlock"})
@CapabilityDescription("Releases a distributed lock of multiple items")
public class HBaseMultipleLockReleaseProcessor extends AbstractHBaseMultipleLockProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship if all locks where released")
            .build();


    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be sent to unlocked")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;


    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(HBASE_CLIENT_SERVICE);
        descriptors.add(TABLE_NAME);
        descriptors.add(COLUMN_FAMILY);
        descriptors.add(COLUMN_QUALIFIER);
        descriptors.add(ROW_ID_ENCODING_STRATEGY);
        descriptors.add(LOCK_ID);


        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String columnFamily = context.getProperty(COLUMN_FAMILY).evaluateAttributeExpressions(flowFile).getValue();
        final String columnQualifier = context.getProperty(COLUMN_QUALIFIER).evaluateAttributeExpressions(flowFile).getValue();
        final String lockId = context.getProperty(LOCK_ID).evaluateAttributeExpressions(flowFile).getValue();
        try {

            final AtomicInteger locks = new AtomicInteger();
            FlowFile finalFlowFile = flowFile;
            clientService.scan(tableName, Collections.singleton(new Column(columnFamily.getBytes(StandardCharsets.UTF_8),
                            lockId.getBytes(StandardCharsets.UTF_8))), "SingleColumnValueFilter('"+columnFamily + "','"+columnQualifier+"',=,'binary:"+lockId +"',false,false)"

                    , 0L, new ResultHandler() {
                        @Override
                        public void handle(byte[] row, ResultCell[] resultCells) {
                            for (ResultCell cell:resultCells
                                 ) {
                                try {
                                    if(clientService.checkAndDelete(tableName,row,getFamilyBytes(cell) ,getQualifierBytes(cell),getValueBytes(cell),
                                            Collections.singleton(new DeleteColumn(getFamilyBytes(cell),getQualifierBytes(cell))))){
                                        locks.incrementAndGet();


                                    }
                                } catch (IOException e) {
                                    getLogger().error("failed to delete job {} from {}",new Object[]{lockId, new String(cell.getRowArray())},e);
                                    FlowFile ff = session.clone(finalFlowFile);
                                    ff =session.putAttribute(ff,"hbase.locks.exception",String.valueOf(e));
                                    session.transfer(ff,REL_FAILURE);
                                }
                            }

                        }
                    });
                flowFile = session.putAttribute(flowFile,"hbase.locks.success",String.valueOf(locks.get()));
                session.transfer(flowFile,REL_SUCCESS);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
