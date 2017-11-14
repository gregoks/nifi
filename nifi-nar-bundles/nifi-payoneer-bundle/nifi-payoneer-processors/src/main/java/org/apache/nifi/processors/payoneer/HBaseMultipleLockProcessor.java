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
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.hbase.increment.IncrementColumn;
import org.apache.nifi.hbase.increment.IncrementColumnResult;
import org.apache.nifi.hbase.increment.IncrementFlowFile;
import org.apache.nifi.hbase.put.PutColumn;
import org.apache.nifi.hbase.put.PutFlowFile;
import org.apache.nifi.json.JsonPathValidator;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
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
@CapabilityDescription("Manages a distributed lock of multiple items")
public class HBaseMultipleLockProcessor extends AbstractHBaseMultipleLockProcessor {

    public static final Relationship REL_ACQUIRED = new Relationship.Builder()
            .name("acquired")
            .description("A FlowFile is routed to this relationship if all locks where acquired")
            .build();

    public static final Relationship REL_NOLOCK = new Relationship.Builder()
            .name("no lock")
            .description("A FlowFile is routed to this relationship if the service could not acquire all the locks")
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
        descriptors.add(TIMESTAMP);
        descriptors.add(LOCK_ID);


        descriptors.add(JSON_PATH);

        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_ACQUIRED);
        relationships.add(REL_NOLOCK);
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
    protected void handleLock(ProcessSession session, FlowFile flowFile, String tableName, String columnFamily, String columnQualifier, String lockId, String rowIdEncodingStrategy, byte[] lockId_bytes, Long timestamp, List<String> lock_ids) {
        long delta =0;
        List<IncrementFlowFile> increments = new ArrayList<>();
        List<PutFlowFile> puts = new ArrayList<>();
        for(String rowId:lock_ids){
            getLogger().info("building lock for {}",new Object[]{rowId});
            byte[] rowKeyBytes = getRow(rowId,rowIdEncodingStrategy);
            IncrementColumn incrementColumn = new IncrementColumn(columnFamily.getBytes(StandardCharsets.UTF_8),
                    columnQualifier.getBytes(StandardCharsets.UTF_8),1L);

            IncrementFlowFile iff = new IncrementFlowFile(tableName,rowKeyBytes,Collections.singletonList(incrementColumn),flowFile);
            increments.add(iff);


            StringBuilder stringBuilder = new StringBuilder("{\"id\":\"")
                    .append(lockId).append("\",\"timestamp\":")
                    .append(timestamp).append("}");

            PutColumn putColumn = new PutColumn(columnFamily.getBytes(StandardCharsets.UTF_8),
                    lockId_bytes,stringBuilder.toString().getBytes(StandardCharsets.UTF_8));
            getLogger().info("Created putColumn for {} {}",new Object[]{rowId,stringBuilder});
            PutFlowFile pff = new PutFlowFile(tableName,rowKeyBytes,Collections.singletonList(putColumn),flowFile);
            puts.add(pff);
        }
        flowFile =session.putAttribute(flowFile,"multi_lock.locks.request",String.valueOf(increments.size()));
        try {
            Collection<IncrementColumnResult> results =clientService.increment(tableName,increments);
            for(IncrementColumnResult icr:results){
                delta+= icr.getValue();
            }
            getLogger().info("Increment result: {}",new Object[]{delta});
            flowFile =session.putAttribute(flowFile,"multi_lock.locks.acquired",String.valueOf(delta));

            if(delta != lock_ids.size()){
                //invalid lock count, need to revert
                revert(session, flowFile, tableName, increments);
            }else{
                //so now we have locks, we must place the job into the cell
                try {
                    clientService.put(tableName, puts);
                    session.transfer(flowFile,REL_ACQUIRED);
                } catch (IOException ex) {
                    //we need to revert
                    revert(session, flowFile, tableName, increments);
                }
            }
        } catch (IOException e) {
            getLogger().error("Error applying lock",e);

            flowFile =session.putAttribute(flowFile,"multi_lock.exception",String.valueOf(e));
            session.transfer(flowFile,REL_FAILURE);
        }
    }

    private void revert(ProcessSession session, FlowFile flowFile, String tableName, List<IncrementFlowFile> increments) throws IOException {

        List<IncrementFlowFile> iccs = new ArrayList<>();
        for(IncrementFlowFile iff:increments){
            List<IncrementColumn> icl = new ArrayList<>();
            for(IncrementColumn ic: iff.getColumns()) {
                icl.add(new IncrementColumn(ic.getColumnFamily(),ic.getColumnQualifier(),-1L));

            }
        iccs.add(new IncrementFlowFile(iff.getTableName(),iff.getRow(),icl,iff.getFlowFile()));
        }
        getLogger().info("Reverting! ({})",new Object[]{increments.size()});
        clientService.increment(tableName,iccs);
        session.transfer(flowFile,REL_NOLOCK);
    }
}
