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

/**
 * Encapsulates the information for one column of a delete operation.
 */
public class DeleteColumn {

    private final byte[] columnFamily;
    private final byte[] columnQualifier;
    private final Long timestamp;


    public DeleteColumn(final byte[] columnFamily, final byte[] columnQualifier) {
        this(columnFamily, columnQualifier, null);
    }

    public DeleteColumn(final byte[] columnFamily, final byte[] columnQualifier, final Long timestamp) {
        this.columnFamily = columnFamily;
        this.columnQualifier = columnQualifier;

        this.timestamp = timestamp;
    }

    public byte[] getColumnFamily() {
        return columnFamily;
    }

    public byte[] getColumnQualifier() {
        return columnQualifier;
    }



    public Long getTimestamp() {
        return timestamp;
    }

}
