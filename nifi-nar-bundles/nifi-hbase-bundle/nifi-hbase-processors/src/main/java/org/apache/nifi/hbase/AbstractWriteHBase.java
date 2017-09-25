package org.apache.nifi.hbase;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.hbase.increment.IncrementFlowFile;
import org.apache.nifi.hbase.put.PutFlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.nio.charset.StandardCharsets;

abstract class AbstractWriteHBase extends AbstractProcessor {

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
    protected static final PropertyDescriptor ROW_ID = new PropertyDescriptor.Builder()
            .name("Row Identifier")
            .description("Specifies the Row ID to use when inserting data into HBase")
            .required(false) // not all sub-classes will require this
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final String STRING_ENCODING_VALUE = "String";
    static final String BYTES_ENCODING_VALUE = "Bytes";
    static final String BINARY_ENCODING_VALUE = "Binary";


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
            .description("The timestamp for the cells being created in HBase. This field can be left blank and HBase will use the current time.")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .build();
    protected static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of FlowFiles to process in a single execution. The FlowFiles will be " +
                    "grouped by table, and a single Put per table will be performed.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("25")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after it has been successfully stored in HBase")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be sent to HBase")
            .build();

    protected HBaseClientService clientService;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        clientService = context.getProperty(HBASE_CLIENT_SERVICE).asControllerService(HBaseClientService.class);
    }
    protected String getTransitUri(String tableName,byte[] row) {
        return "hbase://" + tableName + "/" + new String(row, StandardCharsets.UTF_8);
    }
    protected String getTransitUri(PutFlowFile putFlowFile) {
        return "hbase://" + putFlowFile.getTableName() + "/" + new String(putFlowFile.getRow(), StandardCharsets.UTF_8);
    }
    protected String getTransitUri(IncrementFlowFile putFlowFile) {
        return "hbase://" + putFlowFile.getTableName() + "/" + new String(putFlowFile.getRow(), StandardCharsets.UTF_8);
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
}
