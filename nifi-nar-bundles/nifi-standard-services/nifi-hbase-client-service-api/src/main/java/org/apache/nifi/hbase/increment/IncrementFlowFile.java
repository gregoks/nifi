package org.apache.nifi.hbase.increment;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hbase.put.PutColumn;

import java.util.Collection;

public class IncrementFlowFile {
    private final String tableName;
    private final byte[] row;
    private final Collection<IncrementColumn> columns;
    private final FlowFile flowFile;

    public IncrementFlowFile(String tableName, byte[] row, Collection<IncrementColumn> columns, FlowFile flowFile) {
        this.tableName = tableName;
        this.row = row;
        this.columns = columns;
        this.flowFile = flowFile;
    }

     public String getTableName() {
        return tableName;
    }

    public byte[] getRow() {
        return row;
    }

    public Collection<IncrementColumn> getColumns() {
        return columns;
    }

    public FlowFile getFlowFile() {
        return flowFile;
    }

    public boolean isValid() {
        if (tableName == null || tableName.trim().isEmpty() || null == row || flowFile == null || columns == null || columns.isEmpty()) {
            return false;
        }

        for (IncrementColumn column : columns) {
            if (null == column.getColumnQualifier() || null == column.getColumnFamily() || column.getDelta() == null) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof IncrementFlowFile) {
            IncrementFlowFile iff = (IncrementFlowFile)obj;
            return this.tableName.equals(iff.tableName)
                    && this.row.equals(iff.row)
                    && this.columns.equals(iff.columns)
                    && this.flowFile.equals(iff.flowFile);
        } else {
            return false;
        }
    }
}
