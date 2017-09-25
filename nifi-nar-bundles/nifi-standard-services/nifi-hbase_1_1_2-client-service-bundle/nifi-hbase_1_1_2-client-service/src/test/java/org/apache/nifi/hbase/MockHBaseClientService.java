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
package org.apache.nifi.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.hbase.increment.IncrementColumn;
import org.apache.nifi.hbase.increment.IncrementFlowFile;
import org.apache.nifi.hbase.put.PutColumn;
import org.apache.nifi.hbase.scan.Column;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;

/**
 * Override methods to create a mock service that can return staged data
 */
public class MockHBaseClientService extends HBase_1_1_2_ClientService {

    private Table table;
    private String family;
    private List<Result> results = new ArrayList<>();
    private KerberosProperties kerberosProperties;

    public MockHBaseClientService(final Table table, final String family, final KerberosProperties kerberosProperties) {
        this.table = table;
        this.family = family;
        this.kerberosProperties = kerberosProperties;
    }

    @Override
    protected KerberosProperties getKerberosProperties(File kerberosConfigFile) {
        return kerberosProperties;
    }

    protected void setKerberosProperties(KerberosProperties properties) {
        this.kerberosProperties = properties;

    }

    public void addResult(final String rowKey, final Map<String, String> cells, final long timestamp) {
        final byte[] rowArray = rowKey.getBytes(StandardCharsets.UTF_8);
        final Cell[] cellArray = new Cell[cells.size()];
        int i = 0;
        for (final Map.Entry<String, String> cellEntry : cells.entrySet()) {
            final Cell cell = Mockito.mock(Cell.class);
            when(cell.getRowArray()).thenReturn(rowArray);
            when(cell.getRowOffset()).thenReturn(0);
            when(cell.getRowLength()).thenReturn((short) rowArray.length);

            final String cellValue = cellEntry.getValue();
            final byte[] valueArray = cellValue.getBytes(StandardCharsets.UTF_8);
            when(cell.getValueArray()).thenReturn(valueArray);
            when(cell.getValueOffset()).thenReturn(0);
            when(cell.getValueLength()).thenReturn(valueArray.length);

            final byte[] familyArray = family.getBytes(StandardCharsets.UTF_8);
            when(cell.getFamilyArray()).thenReturn(familyArray);
            when(cell.getFamilyOffset()).thenReturn(0);
            when(cell.getFamilyLength()).thenReturn((byte) familyArray.length);

            final String qualifier = cellEntry.getKey();
            final byte[] qualifierArray = qualifier.getBytes(StandardCharsets.UTF_8);
            when(cell.getQualifierArray()).thenReturn(qualifierArray);
            when(cell.getQualifierOffset()).thenReturn(0);
            when(cell.getQualifierLength()).thenReturn(qualifierArray.length);

            when(cell.getTimestamp()).thenReturn(timestamp);

            cellArray[i++] = cell;
        }

        final Result result = Mockito.mock(Result.class);
        when(result.getRow()).thenReturn(rowArray);
        when(result.rawCells()).thenReturn(cellArray);
        results.add(result);
    }

    @Override
    public void increment(String tableName, byte[] rowId, Collection<IncrementColumn> columns) throws IOException {
        //try (final Table table = connection.getTable(TableName.valueOf(tableName))) {
            Increment inc = new Increment(rowId);
            for (final IncrementColumn column : columns) {
                inc.addColumn(column.getColumnFamily(),
                        column.getColumnQualifier(),
                        column.getDelta());
            }

            table.increment(inc);
        //}
    }

    @Override
    public void increment(String tableName, Collection<IncrementFlowFile> increments) throws IOException {
        // Create one Put per row....
        final Map<String, Increment> rowIncs = new HashMap<>();
        for (final IncrementFlowFile incrementFlowFile : increments) {
            //this is used for the map key as a byte[] does not work as a key.
            final String rowKeyString = new String(incrementFlowFile.getRow(), StandardCharsets.UTF_8);
            Increment inc = rowIncs.get(rowKeyString);
            if (inc == null) {
                inc = new Increment(incrementFlowFile.getRow());
                rowIncs.put(rowKeyString, inc);
            }

            for (final IncrementColumn column : incrementFlowFile.getColumns()) {

                inc.addColumn(
                        column.getColumnFamily(),
                        column.getColumnQualifier(),
                        column.getDelta().longValue());

            }
            table.increment(inc);
        }
    }

    @Override
    public void put(final String tableName, final byte[] rowId, final Collection<PutColumn> columns) throws IOException {
        Put put = new Put(rowId);
        Map<String, String> map = new HashMap<String, String>();
        for (final PutColumn column : columns) {
            put.addColumn(
                    column.getColumnFamily(),
                    column.getColumnQualifier(),
                    column.getBuffer());
            map.put(new String(column.getColumnQualifier()), new String(column.getBuffer()));
        }

        table.put(put);
        addResult(new String(rowId), map, 1);
    }

    @Override
    public boolean checkAndPut(final String tableName, final byte[] rowId, final byte[] family, final byte[] qualifier, final byte[] value, final PutColumn column) throws IOException {
        for (Result result : results) {
            if (Arrays.equals(result.getRow(), rowId)) {
                Cell[] cellArray = result.rawCells();
                for (Cell cell : cellArray) {
                    if (Arrays.equals(cell.getFamilyArray(), family) && Arrays.equals(cell.getQualifierArray(), qualifier)) {
                         if (value == null || Arrays.equals(cell.getValueArray(), value)) {
                             return false;
                         }
                    }
                }
            }
        }

        final List<PutColumn> putColumns = new ArrayList<PutColumn>();
        putColumns.add(column);
        put(tableName, rowId, putColumns);
        return true;
    }

    @Override
    protected ResultScanner getResults(Table table, byte[] startRow, byte[] endRow, Collection<Column> columns) throws IOException {
        final ResultScanner scanner = Mockito.mock(ResultScanner.class);
        Mockito.when(scanner.iterator()).thenReturn(results.iterator());
        return scanner;
    }

    @Override
    protected ResultScanner getResults(Table table, Collection<Column> columns, Filter filter, long minTime) throws IOException {
        final ResultScanner scanner = Mockito.mock(ResultScanner.class);
        Mockito.when(scanner.iterator()).thenReturn(results.iterator());
        return scanner;
    }

    @Override
    protected Connection createConnection(ConfigurationContext context) throws IOException {
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(connection.getTable(table.getName())).thenReturn(table);
        return connection;
    }

}
