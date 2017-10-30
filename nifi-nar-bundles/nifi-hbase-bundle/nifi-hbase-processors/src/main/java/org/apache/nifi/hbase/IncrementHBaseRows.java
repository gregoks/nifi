package org.apache.nifi.hbase;

import com.sun.javaws.exceptions.InvalidArgumentException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.hbase.increment.IncrementColumn;
import org.apache.nifi.hbase.increment.IncrementColumnResult;
import org.apache.nifi.hbase.increment.IncrementFlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.hadoop.exception.RecordReaderFactoryException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.nifi.processor.util.StandardValidators.LONG_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR;

@EventDriven
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"hadoop", "hbase"})
@CapabilityDescription("Increments HBase cells based on attributes of a FlowFile")
public class IncrementHBaseRows extends AbstractWriteHBase {

    protected static final PropertyDescriptor ROW_FIELD_NAME = new PropertyDescriptor.Builder()
            .name("Row Identifier Field Name")
            .description("Specifies the name of a JSON element whose value should be used as the row id for the given JSON document.")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();



    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("The service for reading records from incoming flow files.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    protected static final PropertyDescriptor SKIP_HEAD_LINE = new PropertyDescriptor.Builder()
            .name("Skip head line")
            .description("Set it to true if your first line is the header line e.g. column names")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("Column name Expression that will be incremented by the extracted delta")
                .dynamic(true)
                .required(false)
                .expressionLanguageSupported(true)
                .addValidator(NON_EMPTY_VALIDATOR)
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
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HBASE_CLIENT_SERVICE);
        properties.add(TABLE_NAME);
        properties.add(ROW_ID);
        properties.add(ROW_FIELD_NAME);
        properties.add(ROW_ID_ENCODING_STRATEGY);
        properties.add(COLUMN_FAMILY);
        properties.add(BATCH_SIZE);
        properties.add(RECORD_READER);
        properties.add(SKIP_HEAD_LINE);
        properties.add(RECORD_WRITER);

        return properties;
    }

    protected boolean skipHeadLine;

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        //super.onScheduled(context);
        skipHeadLine = context.getProperty(SKIP_HEAD_LINE).asBoolean();
    }
static String getName(IncrementColumnResult column){

        StringBuilder sb = new StringBuilder()
                .append(new String(column.getColumnFamily(), StandardCharsets.UTF_8))
                .append(':')
                .append(new String(column.getColumnQualifier(), StandardCharsets.UTF_8));


        return sb.toString();

}
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        try {
            if (flowFile == null) return;

            final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
            final String rowId = context.getProperty(ROW_ID).evaluateAttributeExpressions(flowFile).getValue();
            final String rowFieldName = context.getProperty(ROW_FIELD_NAME).evaluateAttributeExpressions(flowFile).getValue();
            final String columnFamily = context.getProperty(COLUMN_FAMILY).evaluateAttributeExpressions(flowFile).getValue();

            final boolean extractRowId = !StringUtils.isBlank(rowFieldName);
            final String rowIdEncodingStrategy = context.getProperty(ROW_ID_ENCODING_STRATEGY).getValue();


            final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
            final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

            final Map<String, String> attributes = new HashMap<>();
            final AtomicInteger recordCount = new AtomicInteger();

            final FlowFile original = flowFile;
            final Map<String, String> originalAttributes = flowFile.getAttributes();
            try {
                flowFile = session.write(flowFile, (in, out) -> {

                    try (final RecordReader reader = readerFactory.createRecordReader(originalAttributes, in, getLogger())) {

                        final RecordSchema writeSchema = writerFactory.getSchema(originalAttributes, reader.getSchema());
                        try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out)) {
                            writer.beginRecordSet();

                            Record record;
                            while ((record = reader.nextRecord()) != null) {
                                //for each record, create a single row increment

                                List<IncrementColumn> columns = new ArrayList<>();

                                for(Map.Entry<PropertyDescriptor,String> entry : context.getProperties().entrySet()){
                                    if(!entry.getKey().isDynamic()) continue;
                                    String columnQualifier = entry.getKey().getName();
                                    String fieldName = context.getProperty(entry.getKey().getName()).evaluateAttributeExpressions(original).getValue();
                                    Long delta =record.getAsLong(fieldName);
                                    if (delta == null) {
                                        getLogger().error("Invalid Delta value for: "+ columnQualifier);
                                        throw new NullPointerException("Invalid Delta value for: "+ columnQualifier);
                                    }
                                    IncrementColumn incrementColumn = new IncrementColumn(columnFamily.getBytes(StandardCharsets.UTF_8),
                                            columnQualifier.getBytes(StandardCharsets.UTF_8),delta);

                                    columns.add(incrementColumn);
                                }

                                String rowId_Value = rowId;
                                if(extractRowId)
                                    rowId_Value = record.getAsString(rowFieldName);
                                byte[] rowKeyBytes = getRow(rowId_Value,rowIdEncodingStrategy);
                                Collection<IncrementColumnResult> results = clientService.increment(tableName,rowKeyBytes,columns);

                                Map<String,Object> resultMap = new HashMap<>();
                                resultMap.put("rowId",rowId_Value);
                                resultMap.put("tableName",tableName);
                                for (IncrementColumnResult res: results) {
                                    resultMap.put(getName(res),res.getValue());

                                }
                                final MapRecord processed = new MapRecord(writeSchema,resultMap,true,true);

                                writer.write(processed);
                            }

                            final WriteResult writeResult = writer.finishRecordSet();
                            attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                            attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                            attributes.putAll(writeResult.getAttributes());
                            recordCount.set(writeResult.getRecordCount());
                        }
                    } catch (final SchemaNotFoundException | MalformedRecordException e) {
                        throw new ProcessException("Could not parse incoming data", e);
                    }
                });
            } catch (final Exception e) {
                getLogger().error("Failed to process {}; will route to failure", new Object[] {flowFile, e});
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            flowFile = session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, REL_SUCCESS);

            final int count = recordCount.get();
            session.adjustCounter("Records Processed", count, false);
            getLogger().info("Successfully converted {} records for {}", new Object[] {count, flowFile});


        } catch (FlowFileAccessException e) {
            getLogger().error("Failed to write due to {}", new Object[]{e},e);
            session.transfer(flowFile, REL_FAILURE);
        } catch (Throwable t) {
            getLogger().error("Failed to write due to {}", new Object[]{t},t);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

}
