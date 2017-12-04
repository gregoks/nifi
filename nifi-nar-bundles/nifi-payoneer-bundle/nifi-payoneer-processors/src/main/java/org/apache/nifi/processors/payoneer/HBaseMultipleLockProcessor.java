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
import org.apache.nifi.hbase.delete.DeleteColumn;
import org.apache.nifi.hbase.increment.IncrementColumn;
import org.apache.nifi.hbase.increment.IncrementColumnResult;
import org.apache.nifi.hbase.increment.IncrementFlowFile;
import org.apache.nifi.hbase.put.PutColumn;
import org.apache.nifi.hbase.put.PutFlowFile;
import org.apache.nifi.hbase.scan.ResultCell;
import org.apache.nifi.hbase.scan.ResultHandler;
import org.apache.nifi.json.JsonPathValidator;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;


import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"hadoop", "hbase","lock"})
@WritesAttribute(attribute="hbase.locks.acquired",description = "the total number of locks acquired")
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

    public static final Relationship REL_UNNLOCK_LOG = new Relationship.Builder()
            .name("unlock log")
            .description("A FlowFile with unlock info will be redirected here")
            .build();

    protected static final PropertyDescriptor LOCK_EXPIRATION = new PropertyDescriptor.Builder()
            .name("Lock expiration")
            .description("When set, releases expired locks on failure")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .required(false)
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
        descriptors.add(TIMESTAMP);
        descriptors.add(LOCK_ID);


        descriptors.add(JSON_PATH);
        descriptors.add(LOCK_EXPIRATION);

        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_ACQUIRED);
        relationships.add(REL_NOLOCK);
        relationships.add(REL_FAILURE);
        relationships.add(REL_UNNLOCK_LOG);
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
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String columnFamily = context.getProperty(COLUMN_FAMILY).evaluateAttributeExpressions(flowFile).getValue();
        final String columnQualifier = context.getProperty(COLUMN_QUALIFIER).evaluateAttributeExpressions(flowFile).getValue();
        final String lockId = context.getProperty(LOCK_ID).evaluateAttributeExpressions(flowFile).getValue();
        final String rowIdEncodingStrategy = context.getProperty(ROW_ID_ENCODING_STRATEGY).getValue();

        final String jsonPath = context.getProperty(JSON_PATH).evaluateAttributeExpressions(flowFile).getValue();

        final String timestampValue = context.getProperty(TIMESTAMP).evaluateAttributeExpressions(flowFile).getValue();
        final String expiredValue = context.getProperty(LOCK_EXPIRATION).evaluateAttributeExpressions(flowFile).getValue();
        Long expiration = null;
        if (!StringUtils.isBlank(expiredValue)) {
            try {
                expiration = Long.valueOf(expiredValue);
            } catch (Exception e) {
                getLogger().error("Invalid expired value: " + expiredValue, e);
                throw new ProcessException(e);
            }
        }

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
            timestamp = null;//new Date().getTime();
        }

        final List<String> lock_ids = new ArrayList<>();
        session.read(flowFile, in -> {
            try (final InputStream bufferedIn = new BufferedInputStream(in)) {
                List<String> locks = JsonPath.read(bufferedIn, jsonPath);
                lock_ids.addAll(locks);
            }
        });

        getLogger().info("Got {} locks ({})", new Object[]{lock_ids.size(), lock_ids});
        try {

            long delta = 0;
            List<String> locked = new ArrayList<>();
            final AtomicInteger locks = new AtomicInteger();
            for (String rowId : lock_ids) {
                getLogger().info("building lock for {}", new Object[]{rowId});
                byte[] rowKeyBytes = getRow(rowId, rowIdEncodingStrategy);


                PutColumn putColumn = new PutColumn(columnFamily.getBytes(StandardCharsets.UTF_8),columnQualifier.getBytes(StandardCharsets.UTF_8),
                        lockId_bytes,timestamp);

                //check if no lock held on the column and set the lock to our lockid (jobid)
                if(clientService.checkAndPut(tableName,rowKeyBytes,columnFamily.getBytes(StandardCharsets.UTF_8),
                        columnQualifier.getBytes(StandardCharsets.UTF_8),null,putColumn)){

                    locked.add(rowId);
                    locks.incrementAndGet();
                }else{
                    //if we could not acquire this lock, first revert all achieved locks
                    release(tableName, columnFamily, columnQualifier, rowIdEncodingStrategy, lockId_bytes, locked, rowId);

                    //
                    if (expiration != null) {
                        releaseExpiredLocks(session,flowFile, tableName, lock_ids, context, expiration);
                    }


                    session.transfer(flowFile, REL_NOLOCK);
                    break;
                }
            }
            flowFile = session.putAttribute(flowFile,"hbase.locks.acquired",String.valueOf(locks.get()));
            session.transfer(flowFile, REL_ACQUIRED);

        } catch (Exception ex) {
            getLogger().error("Could not Acquire lock", ex);
            session.transfer(flowFile, REL_FAILURE);
        }

    }


    private void release(String tableName, String columnFamily, String columnQualifier, String rowIdEncodingStrategy, byte[] lockId_bytes, List<String> locked, String rowId) throws IOException {
        for (String unlockId : locked) {
            getLogger().info("building unlock for {}", new Object[]{unlockId});
            byte[] unlockKeyBytes = getRow(rowId, rowIdEncodingStrategy);


            DeleteColumn delColumn = new DeleteColumn(columnFamily.getBytes(StandardCharsets.UTF_8),columnQualifier.getBytes(StandardCharsets.UTF_8));

            clientService.checkAndDelete(tableName,unlockKeyBytes,columnFamily.getBytes(StandardCharsets.UTF_8),
                    columnQualifier.getBytes(StandardCharsets.UTF_8),lockId_bytes, Collections.singleton(delColumn));


        }

    }



    class InfoLogger {
        final StringBuilder sb = new StringBuilder();
        void info(String msg, Object[] os){
            getLogger().info(msg, os);
            FormattingTuple tp = MessageFormatter.arrayFormat(msg, os);
            sb.append(tp.getMessage()).append("\r\n");

        }

        @Override
        public String toString() {
            return sb.toString();
        }
    }


    private int releaseExpiredLocks(ProcessSession session, FlowFile flowFile, String tableName, List<String> lock_ids, ProcessContext context, Long expiration) throws IOException {

        InfoLogger logger = new InfoLogger();
        byte[] lockQualifier = context.getProperty(COLUMN_QUALIFIER).evaluateAttributeExpressions(flowFile).getValue().getBytes(StandardCharsets.UTF_8);
        final AtomicInteger released = new AtomicInteger();
        for (String lock : lock_ids
                ) {
            byte[] rowIdBytes = getRow(lock, context.getProperty(ROW_ID_ENCODING_STRATEGY).getValue());
            String columnFamily = context.getProperty(COLUMN_FAMILY).evaluateAttributeExpressions(flowFile).getValue();
            Long finalExpiration = expiration;
            final long cutoff = new Date().getTime();
            clientService.scan(tableName, rowIdBytes, rowIdBytes, Collections.emptyList(), new ResultHandler() {
                @Override
                public void handle(byte[] row, ResultCell[] resultCells) {
                    logger.info("got {} cells for row {}",new Object[]{resultCells.length,lock});
                    for (ResultCell cell : resultCells) {
                        logger.info("testing {}:{}",new Object[]{getFamily(cell),getQualifier(cell)});


                        //deal with it
                        logger.info("Date: {}, Timestamp {}, diff:{}",new Object[]{cutoff,cell.getTimestamp(),cutoff - cell.getTimestamp()});
                        if (cutoff - cell.getTimestamp() > finalExpiration) {
                            //now we need to delete
                            try {
                                if (clientService.checkAndDelete(tableName, row,getFamilyBytes(cell) ,getQualifierBytes(cell),getValueBytes(cell),
                                        Collections.singleton(new DeleteColumn(getFamilyBytes(cell) ,getQualifierBytes(cell))))) {
                                    logger.info("Removed expired lock for {} with lock {}",new Object[]{lock,getQualifier(cell)});
                                    released.incrementAndGet();
                                }else{
                                    logger.info("check failed for lock {} cell {}:{}",new Object[]{lock,getFamily(cell),getQualifier(cell)});
                                }
                            } catch (IOException e)
                            {
                                getLogger().warn("Could not clean up expired locks",e);


                            }
                        }
                    }
                }
            });

        }

        FlowFile lff = session.create(flowFile);
        lff = session.putAttribute(lff,"hbase.locks.released",String.valueOf(released.get()));
        session.write(lff, out -> out.write(logger.toString().getBytes(StandardCharsets.UTF_8)));
        session.transfer(lff,REL_UNNLOCK_LOG);

        return released.get();
    }

}
