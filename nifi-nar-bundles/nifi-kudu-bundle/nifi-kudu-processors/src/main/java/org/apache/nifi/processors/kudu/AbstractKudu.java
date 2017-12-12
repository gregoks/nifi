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

package org.apache.nifi.processors.kudu;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kudu.WireProtocol;
import org.apache.kudu.client.*;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.flowfile.FlowFile;

import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.hadoop.KerberosTicketRenewer;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.hadoop.exception.RecordReaderFactoryException;

import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.serialization.record.Record;

import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractKudu extends AbstractProcessor {

    protected static final PropertyDescriptor KUDU_MASTERS = new PropertyDescriptor.Builder()
            .name("Kudu Masters")
            .description("List all kudu masters's ip with port (e.g. 7051), comma separated")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    protected static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the Kudu Table to put data into")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("The service for reading records from incoming flow files.")
            .identifiesControllerService(RecordReaderFactory.class)
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

    protected static final PropertyDescriptor INSERT_OPERATION = new PropertyDescriptor.Builder()
            .name("Insert Operation")
            .description("Specify operationType for this processor. Insert-Ignore will ignore duplicated rows")
            .allowableValues(OperationType.values())
            .defaultValue(OperationType.INSERT.toString())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor FLUSH_MODE = new PropertyDescriptor.Builder()
            .name("Flush Mode")
            .description("Set the new flush mode for a kudu session.\n" +
                    "AUTO_FLUSH_SYNC: the call returns when the operation is persisted, else it throws an exception.\n" +
                    "AUTO_FLUSH_BACKGROUND: the call returns when the operation has been added to the buffer. This call should normally perform only fast in-memory" +
                    " operations but it may have to wait when the buffer is full and there's another buffer being flushed.\n" +
                    "MANUAL_FLUSH: the call returns when the operation has been added to the buffer, else it throws a KuduException if the buffer is full.")
            .allowableValues(SessionConfiguration.FlushMode.values())
            .defaultValue(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND.toString())
            .required(true)
            .build();

    protected static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("Set the number of operations that can be buffered, between 2 - 100000. " +
                    "Depending on your memory size, and data size per row set an appropriate batch size. " +
                    "Gradually increase this number to find out the best one for best performances.")
            .defaultValue("100")
            .required(true)
            .addValidator(StandardValidators.createLongValidator(2, 100000, true))
            .expressionLanguageSupported(true)
            .build();

    protected static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after it has been successfully stored in Kudu")
            .build();
    protected static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be sent to Kudu")
            .build();

    public static final String RECORD_COUNT_ATTR = "record.count";

    protected String kuduMasters;

    protected boolean skipHeadLine;
    protected OperationType operationType;
    protected SessionConfiguration.FlushMode flushMode;
    protected int batchSize = 100;

    protected KuduClient kuduClient;


    /*kerberos*/

    PropertyDescriptor HADOOP_CONF_FILES = new PropertyDescriptor.Builder()
            .name("Hadoop Configuration Files")
            .description("Comma-separated list of Hadoop Configuration files," +
                    " such as kudu-site.xml and core-site.xml for kerberos, " +
                    "including full paths to the files.")
            .addValidator(new ConfigFilesValidator())
            .build();
    static final long TICKET_RENEWAL_PERIOD = 60000;
    private volatile UserGroupInformation ugi;
    private volatile KerberosTicketRenewer renewer;
    protected KerberosProperties kerberosProperties;
    private volatile File kerberosConfigFile = null;

    // Holder of cached Configuration information so validation does not reload the same config over and over
    private final AtomicReference<ValidationResources> validationResourceHolder = new AtomicReference<>();

    @Override
    protected void init(ProcessorInitializationContext context) {
        super.init(context);
        kerberosConfigFile = context.getKerberosConfigurationFile();
        kerberosProperties = getKerberosProperties(kerberosConfigFile);

    }

    protected KerberosProperties getKerberosProperties(File kerberosConfigFile) {
        return new KerberosProperties(kerberosConfigFile);
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        boolean confFileProvided = validationContext.getProperty(HADOOP_CONF_FILES).isSet();

        final List<ValidationResult> problems = new ArrayList<>();
        if (confFileProvided) {
            final String configFiles = validationContext.getProperty(HADOOP_CONF_FILES).getValue();
            ValidationResources resources = validationResourceHolder.get();

            // if no resources in the holder, or if the holder has different resources loaded,
            // then load the Configuration and set the new resources in the holder
            if (resources == null || !configFiles.equals(resources.getConfigResources())) {
                getLogger().debug("Reloading validation resources");
                resources = new ValidationResources(configFiles, getConfigurationFromFiles(configFiles));
                validationResourceHolder.set(resources);
            }

            final Configuration kuduConfig = resources.getConfiguration();
            final String principal = validationContext.getProperty(kerberosProperties.getKerberosPrincipal()).evaluateAttributeExpressions().getValue();
            final String keytab = validationContext.getProperty(kerberosProperties.getKerberosKeytab()).evaluateAttributeExpressions().getValue();

            problems.addAll(KerberosProperties.validatePrincipalAndKeytab(
                    this.getClass().getSimpleName(), kuduConfig, principal, keytab, getLogger()));
        }
        return problems;
    }

    protected Configuration getConfigurationFromFiles(final String configFiles) {
        final Configuration kuduConfig = new Configuration();
        if (StringUtils.isNotBlank(configFiles)) {
            for (final String configFile : configFiles.split(",")) {
                kuduConfig.addResource(new Path(configFile.trim()));
            }
        }
        return kuduConfig;
    }


    @OnScheduled
    public void OnScheduled(final ProcessContext context) throws IOException, InterruptedException {
        try {

            kuduMasters = context.getProperty(KUDU_MASTERS).evaluateAttributeExpressions().getValue();
            if (kuduClient == null) {
                getLogger().debug("Setting up Kudu connection...");
                kuduClient = getKuduConnection(context, kuduMasters);

                getLogger().debug("Kudu connection successfully initialized");
            }

            operationType = OperationType.valueOf(context.getProperty(INSERT_OPERATION).getValue());
            batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();
            flushMode = SessionConfiguration.FlushMode.valueOf(context.getProperty(FLUSH_MODE).getValue());
            skipHeadLine = context.getProperty(SKIP_HEAD_LINE).asBoolean();


            // if we got here then we have a successful connection, so if we have a ugi then start a renewer
            if (ugi != null) {
                final String id = getClass().getSimpleName();
                renewer = SecurityUtil.startTicketRenewalThread(id, ugi, TICKET_RENEWAL_PERIOD, getLogger());
            }
        } catch (KuduException ex) {
            getLogger().error("Exception occurred while interacting with Kudu due to " + ex.getMessage(), ex);
        }
    }

    @OnStopped
    public final void closeClient() throws KuduException {
        if (kuduClient != null) {
            getLogger().info("Closing KuduClient");
            kuduClient.close();
            kuduClient = null;
        }
    }

    private static WireProtocol.AppStatusPB.ErrorCode getErrorCode(Status status) {
        if (status.ok()) return WireProtocol.AppStatusPB.ErrorCode.OK;
        if (status.isAborted()) return WireProtocol.AppStatusPB.ErrorCode.ABORTED;
        if (status.isAlreadyPresent()) return WireProtocol.AppStatusPB.ErrorCode.ALREADY_PRESENT;
        if (status.isConfigurationError()) return WireProtocol.AppStatusPB.ErrorCode.CONFIGURATION_ERROR;
        if (status.isCorruption()) return WireProtocol.AppStatusPB.ErrorCode.CORRUPTION;
        if (status.isEndOfFile()) return WireProtocol.AppStatusPB.ErrorCode.END_OF_FILE;
        if (status.isIllegalState()) return WireProtocol.AppStatusPB.ErrorCode.ILLEGAL_STATE;
        if (status.isIncomplete()) return WireProtocol.AppStatusPB.ErrorCode.INCOMPLETE;
        if (status.isInvalidArgument()) return WireProtocol.AppStatusPB.ErrorCode.INVALID_ARGUMENT;
        if (status.isIOError()) return WireProtocol.AppStatusPB.ErrorCode.IO_ERROR;
        if (status.isNetworkError()) return WireProtocol.AppStatusPB.ErrorCode.NETWORK_ERROR;
        if (status.isNotAuthorized()) return WireProtocol.AppStatusPB.ErrorCode.NOT_AUTHORIZED;
        if (status.isNotFound()) return WireProtocol.AppStatusPB.ErrorCode.NOT_FOUND;
        if (status.isNotSupported()) return WireProtocol.AppStatusPB.ErrorCode.NOT_SUPPORTED;
        if (status.isRemoteError()) return WireProtocol.AppStatusPB.ErrorCode.REMOTE_ERROR;
        if (status.isRuntimeError()) return WireProtocol.AppStatusPB.ErrorCode.RUNTIME_ERROR;
        if (status.isServiceUnavailable()) return WireProtocol.AppStatusPB.ErrorCode.SERVICE_UNAVAILABLE;
        if (status.isTimedOut()) return WireProtocol.AppStatusPB.ErrorCode.TIMED_OUT;
        if (status.isUninitialized()) return WireProtocol.AppStatusPB.ErrorCode.UNINITIALIZED;
        return WireProtocol.AppStatusPB.ErrorCode.UNKNOWN_ERROR;
    }

    static String getErrorString(WireProtocol.AppStatusPB.ErrorCode code) {
        switch (code) {

            case UNKNOWN_ERROR:
                return "UNKNOWN_ERROR";
            case OK:
                return "OK";
            case NOT_FOUND:
                return "NOT_FOUND";
            case CORRUPTION:
                return "CORRUPTION";
            case NOT_SUPPORTED:
                return "NOT_SUPPORTED";
            case INVALID_ARGUMENT:
                return "INVALID_ARGUMENT";
            case IO_ERROR:
                return "IO_ERROR";
            case ALREADY_PRESENT:
                return "ALREADY_PRESENT";
            case RUNTIME_ERROR:
                return "RUNTIME_ERROR";
            case NETWORK_ERROR:
                return "NETWORK_ERROR";
            case ILLEGAL_STATE:
                return "ILLEGAL_STATE";
            case NOT_AUTHORIZED:
                return "NOT_AUTHORIZED";
            case ABORTED:
                return "ABORTED";
            case REMOTE_ERROR:
                return "REMOTE_ERROR";
            case SERVICE_UNAVAILABLE:
                return "SERVICE_UNAVAILABLE";
            case TIMED_OUT:
                return "TIMED_OUT";
            case UNINITIALIZED:
                return "UNINITIALIZED";
            case CONFIGURATION_ERROR:
                return "CONFIGURATION_ERROR";
            case INCOMPLETE:
                return "INCOMPLETE";
            case END_OF_FILE:
                return "END_OF_FILE";
        }
        return "UNKNOWN";
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        KuduSession kuduSession = null;
        try {
            if (flowFile == null) return;
            final Map<String, String> attributes = new HashMap<String, String>();
            final AtomicReference<Throwable> exceptionHolder = new AtomicReference<>(null);
            final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
            final KuduSession finalKuduSession = kuduSession = this.getKuduSession(kuduClient);
            String  tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
            KuduTable kuduTable = this.getKuduTable(kuduClient, tableName);
            session.read(flowFile, (final InputStream rawIn) -> {
                RecordReader recordReader = null;
                try (final BufferedInputStream in = new BufferedInputStream(rawIn)) {
                    try {
                        recordReader = recordReaderFactory.createRecordReader(flowFile, in, getLogger());
                    } catch (Exception ex) {
                        final RecordReaderFactoryException rrfe = new RecordReaderFactoryException("Unable to create RecordReader", ex);
                        exceptionHolder.set(rrfe);
                        return;
                    }

                    List<String> fieldNames = recordReader.getSchema().getFieldNames();
                    final RecordSet recordSet = recordReader.createRecordSet();

                    if (skipHeadLine) recordSet.next();

                    int numOfAddedRecord = 0;
                    Record record = recordSet.next();
                    while (record != null) {
                        org.apache.kudu.client.Operation oper = null;
                        if (operationType == OperationType.UPSERT) {
                            oper = upsertRecordToKudu(kuduTable, record, fieldNames);
                        } else {
                            oper = insertRecordToKudu(kuduTable, record, fieldNames);
                        }
                        finalKuduSession.apply(oper);
                        numOfAddedRecord++;
                        record = recordSet.next();
                    }

                    getLogger().info("KUDU: number of inserted records: " + numOfAddedRecord);
                    attributes.put(RECORD_COUNT_ATTR, String.valueOf(numOfAddedRecord));

                } catch (KuduException ex) {
                    getLogger().error("Exception occurred while interacting with Kudu due to " + ex.getMessage(), ex);
                    exceptionHolder.set(ex);
                } catch (Exception e) {
                    exceptionHolder.set(e);
                } finally {
                    IOUtils.closeQuietly(recordReader);
                }
            });
            List<OperationResponse> responses = kuduSession.flush();


            if (exceptionHolder.get() != null) {
                throw exceptionHolder.get();
            }

            Map<WireProtocol.AppStatusPB.ErrorCode, AtomicInteger> errors = new HashMap<>();
            for (OperationResponse resp : responses) {
                if (resp.hasRowError()) {
                    RowError e = resp.getRowError();
                    WireProtocol.AppStatusPB.ErrorCode errorCode = getErrorCode(e.getErrorStatus());
                    AtomicInteger atomicInteger = errors.get(errorCode);
                    if (atomicInteger == null) {
                        atomicInteger = new AtomicInteger();
                        errors.put(errorCode, atomicInteger);
                    }
                    atomicInteger.incrementAndGet();
                }
            }
            for (Map.Entry<WireProtocol.AppStatusPB.ErrorCode, AtomicInteger> entry : errors.entrySet()) {
                attributes.put("kudu.errors." + getErrorString(entry.getKey()), String.valueOf(entry.getValue().get()));
            }

            // Update flow file's attributes after the ingestion
            session.putAllAttributes(flowFile, attributes);

            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().send(flowFile, "Successfully added flowfile to kudu");

        } catch (IOException | FlowFileAccessException e) {
            getLogger().error("Failed to write due to {}", new Object[]{e}, e);
            session.transfer(flowFile, REL_FAILURE);
        } catch (Throwable t) {
            getLogger().error("Failed to write due to {}", new Object[]{t}, t);
            session.transfer(flowFile, REL_FAILURE);
        } finally {
            if (kuduSession != null) {
                try {
                    kuduSession.close();
                } catch (KuduException e) {
                    getLogger().warn("failed to close kudu session due to {}", new Object[]{e}, e);
                }
            }
        }
    }

    protected KuduClient getKuduConnection(ProcessContext context, String kuduMasters) throws IOException, InterruptedException {
        final String configFiles = context.getProperty(HADOOP_CONF_FILES).getValue();
        final Configuration kuduConfig = getConfigurationFromFiles(configFiles);

        tables.clear(); //reset tables on new client

        if (SecurityUtil.isSecurityEnabled(kuduConfig)) {
            final String principal = context.getProperty(kerberosProperties.getKerberosPrincipal()).evaluateAttributeExpressions().getValue();
            final String keyTab = context.getProperty(kerberosProperties.getKerberosKeytab()).evaluateAttributeExpressions().getValue();

            getLogger().info("Kudu Security Enabled, logging in as principal {} with keytab {}", new Object[] {principal, keyTab});
            ugi = SecurityUtil.loginKerberos(kuduConfig, principal, keyTab);
            getLogger().info("Successfully logged in as principal {} with keytab {}", new Object[] {principal, keyTab});

            return ugi.doAs((PrivilegedExceptionAction<KuduClient>)
                    () -> new KuduClient.KuduClientBuilder(kuduMasters).build());

        } else {
            return new KuduClient.KuduClientBuilder(kuduMasters)
                    .build();
        }

    }

    Map<String,KuduTable> tables = new HashMap<>();

    protected KuduTable getKuduTable(KuduClient client, String tableName) throws KuduException {
        synchronized (tables){
            KuduTable table = tables.get(tableName);
            if(table == null)
            {
                table =client.openTable(tableName);
                tables.put(tableName,table);
            }
            return table;
        }

    }

    protected KuduSession getKuduSession(KuduClient client) {

        KuduSession kuduSession = client.newSession();

        kuduSession.setMutationBufferSpace(batchSize);
        kuduSession.setFlushMode(flushMode);

        if (operationType == OperationType.INSERT_IGNORE) {
            kuduSession.setIgnoreAllDuplicateRows(true);
        }

        return kuduSession;
    }

    protected abstract Insert insertRecordToKudu(final KuduTable table, final Record record, final List<String> fields) throws Exception;

    protected abstract Upsert upsertRecordToKudu(final KuduTable table, final Record record, final List<String> fields) throws Exception;
}

