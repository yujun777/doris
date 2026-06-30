// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.catalog;

import org.apache.doris.binlog.BinlogTestUtils;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public class OlapTableRowBinlogSchemaTest {

    private static OlapTable newTestTable(BinlogConfig binlogConfig) {
        long baseIndexId = 1L;
        Column key = new Column("k1", PrimitiveType.INT);
        key.setIsKey(true);
        Column value = new Column("v1", PrimitiveType.INT);
        value.setIsKey(false);
        List<Column> baseSchema = Lists.newArrayList(key, value);

        // Construct a minimal olap table for row binlog schema generation.
        OlapTable table = new OlapTable(1L, "tbl", baseSchema, KeysType.PRIMARY_KEYS, null, null);
        table.setBaseIndexId(baseIndexId);
        MaterializedIndexMeta baseIndexMeta = new MaterializedIndexMeta(baseIndexId, baseSchema, 1, 1, (short) 1,
                TStorageType.COLUMN, KeysType.PRIMARY_KEYS, null);
        table.addIndexIdToMetaForUnitTest(baseIndexId, baseIndexMeta);
        table.addIndexNameToIdForUnitTest("base", baseIndexId);
        table.setBinlogConfig(binlogConfig);

        if (binlogConfig.isEnableForStreaming()) {
            // Mock row binlog meta by using generated schema to make getRowBinlogMeta() work in pure unit test.
            long rowBinlogIndexId = 2L;
            List<Column> rowBinlogSchema = table.generateTableRowBinlogSchema();
            MaterializedIndexMeta rowBinlogMeta = new MaterializedIndexMeta(rowBinlogIndexId, rowBinlogSchema, 1, 1,
                    (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS, null, null, null, null);
            rowBinlogMeta.initSchemaColumnUniqueId();
            table.setRowBinlogMeta(rowBinlogMeta, "row_binlog");
        }
        return table;
    }

    private static MTMV newTestIvmMtmv(BinlogConfig binlogConfig) {
        long baseIndexId = 1L;
        Column key = new Column("k1", PrimitiveType.INT);
        key.setIsKey(true);
        Column rowId = new Column(Column.IVM_ROW_ID_COL, ScalarType.createType(PrimitiveType.LARGEINT),
                true, null, false, "ivm row id hidden column", false);
        Column value = new Column("v1", PrimitiveType.INT);
        value.setIsKey(false);
        Column aggState = new Column(Column.IVM_HIDDEN_COLUMN_PREFIX + "AGG_0_SUM_COL__", PrimitiveType.BIGINT);
        aggState.setIsVisible(false);
        aggState.setIsKey(false);
        List<Column> baseSchema = Lists.newArrayList(key, rowId, aggState, value);

        OlapTableFactory.MTMVParams params = new OlapTableFactory.MTMVParams();
        params.tableId = 1L;
        params.tableName = "mv";
        params.schema = baseSchema;
        params.keysType = KeysType.UNIQUE_KEYS;
        params.partitionInfo = new SinglePartitionInfo();
        params.distributionInfo = new RandomDistributionInfo(1);
        params.enableIvm = true;
        params.ivmPlanSignature = "test";
        MTMV mtmv = new MTMV(params);
        mtmv.setBaseIndexId(baseIndexId);
        mtmv.setIndexMeta(baseIndexId, "mv", baseSchema, 1, 1, (short) 1,
                TStorageType.COLUMN, KeysType.UNIQUE_KEYS);
        mtmv.setEnableUniqueKeyMergeOnWrite(true);
        mtmv.setBinlogConfig(binlogConfig);
        return mtmv;
    }

    @Test
    public void testRowBinlogSchemaOnEnable() {
        OlapTable tableWithoutBefore = newTestTable(BinlogTestUtils.newTestRowBinlogConfig(true, false));
        Assertions.assertTrue(tableWithoutBefore.needRowBinlog());
        List<String> tableWithoutBeforeColumns =
                tableWithoutBefore.getRowBinlogMeta().getSchema(true).stream().map(Column::getName)
                        .collect(Collectors.toList());
        Assertions.assertFalse(tableWithoutBeforeColumns.contains(Column.generateBeforeColName("v1")));
        Assertions.assertEquals(tableWithoutBeforeColumns.indexOf(Column.BINLOG_LSN_COL), 2);
        Assertions.assertEquals(tableWithoutBeforeColumns.indexOf(Column.BINLOG_OPERATION_COL), 3);
        Assertions.assertEquals(tableWithoutBeforeColumns.indexOf(Column.BINLOG_TIMESTAMP_COL), 4);
        Assertions.assertEquals(tableWithoutBeforeColumns.size(), 5);

        OlapTable tableWithBefore = newTestTable(BinlogTestUtils.newTestRowBinlogConfig(true, true));
        Assertions.assertTrue(tableWithBefore.needRowBinlog());
        List<String> tableWithBeforeColumns =
                tableWithBefore.getRowBinlogMeta().getSchema(true).stream().map(Column::getName)
                        .collect(Collectors.toList());
        Assertions.assertTrue(tableWithBeforeColumns.contains(Column.generateBeforeColName("v1")));
        Assertions.assertEquals(tableWithBeforeColumns.indexOf(Column.BINLOG_LSN_COL), 3);
        Assertions.assertEquals(tableWithBeforeColumns.indexOf(Column.BINLOG_OPERATION_COL), 4);
        Assertions.assertEquals(tableWithBeforeColumns.indexOf(Column.BINLOG_TIMESTAMP_COL), 5);
        Assertions.assertEquals(tableWithBeforeColumns.size(), 6);
    }

    @Test
    public void testRowBinlogSchemaOnDisable() {
        OlapTable table = newTestTable(BinlogTestUtils.newTestRowBinlogConfig(false, false));
        Assertions.assertFalse(table.needRowBinlog());
        Assertions.assertTrue(table.getBaseIndexMeta().getRowBinlogIndexId() <= 0);
    }

    @Test
    public void testIvmRowBinlogSchemaIncludesHiddenKeyColumns() {
        MTMV mtmv = newTestIvmMtmv(BinlogTestUtils.newTestRowBinlogConfig(true, true));

        List<Column> rowBinlogSchema = mtmv.generateTableRowBinlogSchema();
        List<String> columnNames = rowBinlogSchema.stream().map(Column::getName).collect(Collectors.toList());

        Assertions.assertEquals("k1", columnNames.get(0));
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, columnNames.get(1));
        Assertions.assertFalse(rowBinlogSchema.get(1).isVisible());
        Assertions.assertTrue(rowBinlogSchema.get(1).isKey());
        Assertions.assertEquals("v1", columnNames.get(2));
        Assertions.assertEquals(Column.generateBeforeColName("v1"), columnNames.get(3));
        Assertions.assertFalse(columnNames.contains(Column.IVM_HIDDEN_COLUMN_PREFIX + "AGG_0_SUM_COL__"));
        Assertions.assertFalse(columnNames.contains(Column.generateBeforeColName(Column.IVM_ROW_ID_COL)));
        Assertions.assertEquals(4, columnNames.indexOf(Column.BINLOG_LSN_COL));
        Assertions.assertEquals(5, columnNames.indexOf(Column.BINLOG_OPERATION_COL));
        Assertions.assertEquals(6, columnNames.indexOf(Column.BINLOG_TIMESTAMP_COL));
        Assertions.assertEquals(7, columnNames.size());
    }
}
