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
package org.apache.nifi.hbase.delete;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hbase.AbstractActionFlowFile;

import java.util.Collection;

/**
 * Wrapper to encapsulate all of the information for the Put along with the FlowFile.
 */
public class DeleteFlowFile extends AbstractActionFlowFile {


    private final byte[] family;
    private final  byte[] qualifier;
    private final byte[] value;
    private final Collection<DeleteColumn> columns;


    public DeleteFlowFile(String tableName, byte[] row, byte[] family, byte[] qualifier, byte[] value, Collection<DeleteColumn> columns, FlowFile flowFile) {
    super(tableName, row, flowFile);
        this.family = family;
        this.qualifier = qualifier;
        this.value = value;
        this.columns = columns;

    }

    public byte[] getFamily() {
        return family;
    }

    public byte[] getQualifier() {
        return qualifier;
    }

    public byte[] getValue() {
        return value;
    }

    public Collection<DeleteColumn> getColumns() {
        return columns;
    }

    public boolean isValid() {
        if (!super.isValid() ||family == null || qualifier == null ||  columns == null || columns.isEmpty()) {
            return false;
        }

        for (DeleteColumn column : columns) {
            if (null == column.getColumnQualifier() || null == column.getColumnFamily() ) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DeleteFlowFile) {
            DeleteFlowFile pff = (DeleteFlowFile)obj;
            return super.equals(obj)
                    &&this.family.equals(pff.family)
                    &&this.qualifier.equals(pff.qualifier)
                    &&((this.value == null && pff.value == null) ||(this.value != null && this.value.equals(pff.value)))
                    && this.columns.equals(pff.columns);
        } else {
            return false;
        }
    }
}
