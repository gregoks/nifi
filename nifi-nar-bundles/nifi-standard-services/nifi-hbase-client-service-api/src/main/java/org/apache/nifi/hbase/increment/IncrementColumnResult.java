package org.apache.nifi.hbase.increment;

import java.nio.charset.StandardCharsets;

public class IncrementColumnResult {
    private final byte[] columnFamily;
    private final byte[] columnQualifier;
    private final Long value;

    public IncrementColumnResult(final byte[] columnFamily, final byte[] columnQualifier, final Long value) {
        this.columnFamily = columnFamily;
        this.columnQualifier = columnQualifier;
        this.value = value;
    }

    public byte[] getColumnFamily() {
        return columnFamily;
    }

    public byte[] getColumnQualifier() {
        return columnQualifier;
    }

    public Long getValue() {
        return value;
    }



    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder().append('"')
                .append(new String(columnFamily, StandardCharsets.UTF_8))
                .append(':')
                .append(new String(columnQualifier, StandardCharsets.UTF_8))
                .append('"')
                .append(':')
                .append(value);

        return sb.toString();
    }
}
