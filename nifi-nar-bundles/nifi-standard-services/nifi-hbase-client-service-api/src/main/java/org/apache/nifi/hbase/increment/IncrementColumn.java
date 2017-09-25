package org.apache.nifi.hbase.increment;

public class IncrementColumn {
    private final byte[] columnFamily;
    private final byte[] columnQualifier;
    private final Long delta;

    public IncrementColumn(final byte[] columnFamily, final byte[] columnQualifier, final Long delta) {
        this.columnFamily = columnFamily;
        this.columnQualifier = columnQualifier;
        this.delta = delta;
    }

    public byte[] getColumnFamily() {
        return columnFamily;
    }

    public byte[] getColumnQualifier() {
        return columnQualifier;
    }

    public Long getDelta() {
        return delta;
    }



}
