package org.apache.nifi.hbase;

import org.apache.nifi.flowfile.FlowFile;

public abstract class AbstractActionFlowFile {
    private final String tableName;
    private final byte[] row;
    private final FlowFile flowFile;

    protected AbstractActionFlowFile(String tableName, byte[] row, FlowFile flowFile) {
        this.tableName = tableName;
        this.row = row;
        this.flowFile = flowFile;
    }


    public String getTableName() {
        return tableName;
    }

    public byte[] getRow() {
        return row;
    }
    public FlowFile getFlowFile() {
        return flowFile;
    }

    public boolean isValid() {
        if (tableName == null || tableName.trim().isEmpty() || null == row || flowFile == null ) {
            return false;
        }
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AbstractActionFlowFile) {
            AbstractActionFlowFile aff = (AbstractActionFlowFile)obj;
            return this.tableName.equals(aff.tableName)
                    && this.row.equals(aff.row)
                    && this.flowFile.equals(aff.flowFile);
        } else {
            return false;
        }
    }

}
