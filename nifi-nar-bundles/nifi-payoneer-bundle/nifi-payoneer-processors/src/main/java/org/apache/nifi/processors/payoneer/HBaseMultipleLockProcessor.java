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
                List<String> locks = JsonPath.read(bufferedIn, jsonPath);
                lock_ids.addAll(locks);
            }
        });

        getLogger().info("Got {} locks ({})", new Object[]{lock_ids.size(), lock_ids});
        try {

            long delta = 0;
            List<IncrementFlowFile> increments = new ArrayList<>();
            List<PutFlowFile> puts = new ArrayList<>();
            for (String rowId : lock_ids) {
                getLogger().info("building lock for {}", new Object[]{rowId});
                byte[] rowKeyBytes = getRow(rowId, rowIdEncodingStrategy);
                IncrementColumn incrementColumn = new IncrementColumn(columnFamily.getBytes(StandardCharsets.UTF_8),
                        columnQualifier.getBytes(StandardCharsets.UTF_8), 1L);

                IncrementFlowFile iff = new IncrementFlowFile(tableName, rowKeyBytes, Collections.singletonList(incrementColumn), flowFile);
                increments.add(iff);


                StringBuilder stringBuilder = new StringBuilder("{\"id\":\"")
                        .append(lockId).append("\",\"timestamp\":")
                        .append(timestamp).append("}");

                PutColumn putColumn = new PutColumn(columnFamily.getBytes(StandardCharsets.UTF_8),
                        lockId_bytes, stringBuilder.toString().getBytes(StandardCharsets.UTF_8));
                getLogger().info("Created putColumn for {} {}", new Object[]{rowId, stringBuilder});
                PutFlowFile pff = new PutFlowFile(tableName, rowKeyBytes, Collections.singletonList(putColumn), flowFile);
                puts.add(pff);
            }
            flowFile = session.putAttribute(flowFile, "multi_lock.locks.request", String.valueOf(increments.size()));
            try {
                Collection<IncrementColumnResult> results = clientService.increment(tableName, increments);
                for (IncrementColumnResult icr : results) {
                    delta += icr.getValue();
                }
                getLogger().info("Increment result: {}", new Object[]{delta});
                flowFile = session.putAttribute(flowFile, "multi_lock.locks.acquired", String.valueOf(delta));

                if (delta != lock_ids.size()) {
                    //invalid lock count, need to revert
                    revert(session, flowFile, tableName, increments, lock_ids, context);


                } else {
                    //so now we have locks, we must place the job into the cell
                    try {
                        clientService.put(tableName, puts);
                        session.transfer(flowFile, REL_ACQUIRED);
                    } catch (IOException ex) {
                        //we need to revert
                         revert(session, flowFile, tableName, increments, lock_ids, context);

                    }
                }
            } catch (IOException e) {
                getLogger().error("Error applying lock", e);

                flowFile = session.putAttribute(flowFile, "multi_lock.exception", String.valueOf(e));
                session.transfer(flowFile, REL_FAILURE);
            }
        } catch (Exception ex) {
            getLogger().error("Could not Acquire lock", ex);
            session.transfer(flowFile, REL_FAILURE);
        }

    }

    private int revert(ProcessSession session, FlowFile flowFile, String tableName, List<IncrementFlowFile> increments
            , List<String> lock_ids, ProcessContext context) throws IOException {

        List<IncrementFlowFile> iccs = new ArrayList<>();
        for (IncrementFlowFile iff : increments) {
            List<IncrementColumn> icl = new ArrayList<>();
            for (IncrementColumn ic : iff.getColumns()) {
                icl.add(new IncrementColumn(ic.getColumnFamily(), ic.getColumnQualifier(), -1L));

            }
            iccs.add(new IncrementFlowFile(iff.getTableName(), iff.getRow(), icl, iff.getFlowFile()));
        }
        getLogger().info("Reverting! ({})", new Object[]{increments.size()});
        clientService.increment(tableName, iccs);


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
        int unlocked = 0;
        //now release old
        if (expiration != null) {
            unlocked = releaseExpiredLocks(session,flowFile, tableName, lock_ids, context, expiration);
            session.putAttribute(flowFile,"locks.released",String.valueOf(unlocked));
        }

        session.transfer(flowFile, REL_NOLOCK);
        return unlocked;
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

    static String getQualifier(ResultCell cell){
        return new String(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength(),StandardCharsets.UTF_8);
    }
    static String getFamily(ResultCell cell){
        return new String(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength(),StandardCharsets.UTF_8);
    }
    static byte[] getValueBytes(ResultCell cell){
        return Arrays.copyOfRange(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
    }

    static byte[] getFamilyBytes(ResultCell cell){
        return Arrays.copyOfRange(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
    }

    static byte[] getQualifierBytes(ResultCell cell){
        return Arrays.copyOfRange(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
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
                        if (Arrays.equals(cell.getQualifierArray(),lockQualifier) || !columnFamily.equals(getFamily(cell)))
                            continue;
                        //deal with it
                        logger.info("Date: {}, Timestamp {}, diff:{}",new Object[]{cutoff,cell.getTimestamp(),cutoff - cell.getTimestamp()});
                        if (cutoff - cell.getTimestamp() > finalExpiration) {
                            //now we need to delete
                            try {
                                if (clientService.checkAndDelete(tableName, row,getFamilyBytes(cell) ,getQualifierBytes(cell),getValueBytes(cell),
                                        Collections.singleton(new DeleteColumn(cell.getFamilyArray(), cell.getQualifierArray())))) {
                                    clientService.increment(tableName, cell.getRowArray(), Collections.singleton(new IncrementColumn(columnFamily.getBytes(StandardCharsets.UTF_8)
                                            , lockQualifier, -1L)));
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
        session.write(lff, out -> out.write(logger.toString().getBytes(StandardCharsets.UTF_8)));
        session.transfer(lff,REL_UNNLOCK_LOG);

        return released.get();
    }

}
