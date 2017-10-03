package org.apache.nifi.hbase;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hbase.io.JsonRowSerializer;
import org.apache.nifi.hbase.io.RowSerializer;
import org.apache.nifi.hbase.scan.ResultCell;
import org.apache.nifi.hbase.scan.ResultHandler;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@TriggerWhenEmpty
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"hbase", "get", "ingest"})
@CapabilityDescription("This Processor scans HBase for any records in the specified table based on the given filters. (Filter Language can be found at: https://issues.apache.org/jira/browse/HBASE-4176)" +
        "Each record is output in JSON format, as "
        + "{\"row\": \"<row key>\", \"cells\": { \"<column 1 family>:<column 1 qualifier>\": \"<cell 1 value>\", \"<column 2 family>:<column 2 qualifier>\": \"<cell 2 value>\", ... }}. "
        + "For each record received, a Provenance RECEIVE event is emitted with the format hbase://<table name>/<row key>, where <row key> is the UTF-8 encoded value of the row's key.")
@WritesAttributes({
        @WritesAttribute(attribute = "hbase.table", description = "The name of the HBase table that the data was pulled from"),
        @WritesAttribute(attribute = "mime.type", description = "Set to application/json to indicate that output is JSON")
})
public class ScanHBase extends AbstractScanHBase {

    static final Set<Relationship> relationships;
    static {
        Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);

        relationships = Collections.unmodifiableSet(rels);
    }


    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HBASE_CLIENT_SERVICE);
        properties.add(TABLE_NAME_EXP);
        properties.add(COLUMNS_EXP);
        properties.add(FILTER_EXPRESSION_EXP);
        properties.add(INITIAL_TIMERANGE);
        properties.add(CHARSET);
        return properties;
    }



    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        AtomicReference<FlowFile> flowFile = new AtomicReference<>(session.get());
        if (flowFile.get() == null) {
            return;
        }
        final String tableName = context.getProperty(TABLE_NAME_EXP).evaluateAttributeExpressions(flowFile.get()).getValue();
        if (StringUtils.isBlank(tableName)) {
            getLogger().error("Table Name is blank or null for {}, transferring to failure", new Object[] {flowFile});
            session.transfer(session.penalize(flowFile.get()), REL_FAILURE);
            return;
        }


        final String initialTimeRange = context.getProperty(INITIAL_TIMERANGE).getValue();
        final String filterExpression = context.getProperty(FILTER_EXPRESSION_EXP).evaluateAttributeExpressions(flowFile.get()).getValue();
        final HBaseClientService hBaseClientService = context.getProperty(HBASE_CLIENT_SERVICE).asControllerService(HBaseClientService.class);

        try {
            final Charset charset = Charset.forName(context.getProperty(CHARSET).getValue());
            final RowSerializer serializer = new JsonRowSerializer(charset);


            final long minTime = (initialTimeRange.equals(NONE.getValue()) ? 0L : System.currentTimeMillis());


            final Map<String, Set<String>> cellsMatchingTimestamp = new HashMap<>();

            final AtomicLong rowsPulledHolder = new AtomicLong();
            final AtomicReference<Long> latestTimestampHolder = new AtomicReference<>(minTime);
            flowFile.set(session.write(flowFile.get(), (out) -> {
                out.write("[\r\n".getBytes(charset));

            }));


            hBaseClientService.scan(tableName, columns, filterExpression, minTime, new ResultHandler() {
                @Override
                public void handle(final byte[] rowKey, final ResultCell[] resultCells) {

                    final String rowKeyString = new String(rowKey, StandardCharsets.UTF_8);

                    // check if latest cell timestamp is equal to our cutoff.
                    // if any of the cells have a timestamp later than our cutoff, then we
                    // want the row. But if the cell with the latest timestamp is equal to
                    // our cutoff, then we want to check if that's one of the cells that
                    // we have already seen.
                    long latestCellTimestamp = 0L;
                    for (final ResultCell cell : resultCells) {
                        if (cell.getTimestamp() > latestCellTimestamp) {
                            latestCellTimestamp = cell.getTimestamp();
                        }
                    }

                    // we've already seen this.
                    if (latestCellTimestamp < minTime) {
                        getLogger().debug("latest cell timestamp for row {} is {}, which is earlier than the minimum time of {}",
                                new Object[] {rowKeyString, latestCellTimestamp, minTime});
                        return;
                    }



                    if(rowsPulledHolder.get() >0)
                    {
                        flowFile.set(session.append(flowFile.get(), new OutputStreamCallback() {
                            @Override
                            public void process(OutputStream out) throws IOException {
                                out.write("\r\n, ".getBytes(charset));
                            }
                        }));
                    }

                    flowFile.set(session.append(flowFile.get(), new OutputStreamCallback() {
                        @Override
                        public void process(OutputStream out) throws IOException {
                            serializer.serialize(rowKey, resultCells, out);
                        }
                    }));


                    final String transitUri = "hbase://" + tableName + "/" + rowKeyString;

                        session.getProvenanceReporter().fetch(flowFile.get(), transitUri);
                    getLogger().debug("Received {} from HBase with row key {}", new Object[]{flowFile, rowKeyString});

                    // we could potentially have a huge number of rows. If we get to 500, go ahead and commit the
                    // session so that we can avoid buffering tons of FlowFiles without ever sending any out.
                    rowsPulledHolder.addAndGet(1);
                }
            });
            flowFile.set(session.append(flowFile.get(), new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    out.write("\r\n]".getBytes(charset));
                }
            }));
            final Map<String, String> attributes = new HashMap<>();
            attributes.put("hbase.table", tableName);
            attributes.put("mime.type", "application/json");
            flowFile.set(session.putAllAttributes(flowFile.get(), attributes));

            //session.getProvenanceReporter().receive(flowFile, "hbase://" + tableName + "/" + rowKeyString);
            session.transfer(flowFile.get(), REL_SUCCESS);

            final GetHBase.ScanResult scanResults = new GetHBase.ScanResult(latestTimestampHolder.get(), cellsMatchingTimestamp);

            // Commit session before we replace the lastResult; if session commit fails, we want
            // to pull these records again.
            session.commit();
        } catch (final IOException e) {
            getLogger().error("Failed to receive data from HBase due to {}", e);
            session.rollback();
            // if we failed, we want to yield so that we don't hammer hbase.
            context.yield();
        }
    }
}
