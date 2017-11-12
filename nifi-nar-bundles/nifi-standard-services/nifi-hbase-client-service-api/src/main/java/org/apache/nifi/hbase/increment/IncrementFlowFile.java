package org.apache.nifi.hbase.increment;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hbase.AbstractActionFlowFile;
import org.apache.nifi.hbase.put.PutColumn;

import java.util.ArrayList;
import java.util.Collection;

public class IncrementFlowFile extends AbstractActionFlowFile {


    private final Collection<IncrementColumn> columns;
    private final Collection<IncrementColumnResult> columnResults=new ArrayList<>();


    public IncrementFlowFile(String tableName, byte[] row, Collection<IncrementColumn> columns, FlowFile flowFile) {
        super(tableName, row, flowFile);
        this.columns = columns;

    }

    public IncrementFlowFile setColumnResults(Collection<IncrementColumnResult> results){
        columnResults.clear();
        columnResults.addAll(results);
        return this;
    }

    public Collection<IncrementColumnResult> getColumnResults() {
        return columnResults;
    }


    public Collection<IncrementColumn> getColumns() {
        return columns;
    }



    public boolean isValid() {
        if (!super.isValid() || columns == null || columns.isEmpty()) {
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
            return super.equals(obj)
                    && this.columns.equals(iff.columns);
        } else {
            return false;
        }
    }
}
