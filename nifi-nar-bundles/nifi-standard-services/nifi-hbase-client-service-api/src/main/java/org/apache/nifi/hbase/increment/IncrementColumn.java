package org.apache.nifi.hbase.increment;

import java.nio.charset.StandardCharsets;

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


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder().append('"')
                .append(new String(columnFamily, StandardCharsets.UTF_8))
                .append(':')
                .append(new String(columnQualifier, StandardCharsets.UTF_8))
                .append('"')
                .append(':')
                .append(delta);

        return sb.toString();
    }
}
