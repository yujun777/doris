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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MurmurHash364;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Uuid;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class IVMCreateSqlRewriterTest {

    @Test
    public void testRewriteForCreateUsesHashRowIdForMowUniqueKeyScan(@Mocked OlapTable olapTable)
            throws Exception {
        Column keyColumn = new Column("k1", PrimitiveType.INT);
        new Expectations() {
            {
                olapTable.getPartitionIds();
                result = ImmutableList.of();
                minTimes = 0;
                olapTable.getBaseIndexId();
                result = 1L;
                minTimes = 0;
                olapTable.getKeysType();
                result = KeysType.UNIQUE_KEYS;
                minTimes = 0;
                olapTable.getEnableUniqueKeyMergeOnWrite();
                result = true;
                minTimes = 0;
                olapTable.getBaseSchema(true);
                result = ImmutableList.of(keyColumn);
                minTimes = 0;
                olapTable.getBaseSchemaKeyColumns();
                result = ImmutableList.of(keyColumn);
                minTimes = 0;
                olapTable.getName();
                result = "mow_tbl";
                minTimes = 0;
            }
        };

        LogicalOlapScan scan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), olapTable,
                ImmutableList.of("ctl", "db"));
        Plan rewritten = new IVMCreateSqlRewriter().rewriteForCreate(scan);

        Assertions.assertInstanceOf(LogicalProject.class, rewritten);
        NamedExpression rowIdExpr = ((LogicalProject<?>) rewritten).getProjects().get(1);
        Assertions.assertInstanceOf(Alias.class, rowIdExpr);
        Assertions.assertInstanceOf(MurmurHash364.class, ((Alias) rowIdExpr).child());
    }

    @Test
    public void testRewriteForCreateRejectsMorUniqueKeyScan(@Mocked OlapTable olapTable) {
        new Expectations() {
            {
                olapTable.getPartitionIds();
                result = ImmutableList.of();
                minTimes = 0;
                olapTable.getBaseIndexId();
                result = 1L;
                minTimes = 0;
                olapTable.getKeysType();
                result = KeysType.UNIQUE_KEYS;
                minTimes = 0;
                olapTable.getEnableUniqueKeyMergeOnWrite();
                result = false;
                minTimes = 0;
                olapTable.getName();
                result = "mor_tbl";
                minTimes = 0;
            }
        };

        LogicalOlapScan scan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), olapTable,
                ImmutableList.of("ctl", "db"));
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> new IVMCreateSqlRewriter().rewriteForCreate(scan));
        Assertions.assertTrue(exception.getMessage().contains("Merge-on-read unique key table"));
    }

    @Test
    public void testRewriteForCreateUsesUuidRowIdForDupKeyScan(@Mocked OlapTable olapTable) throws Exception {
        Column valueColumn = new Column("v1", PrimitiveType.INT);
        new Expectations() {
            {
                olapTable.getPartitionIds();
                result = ImmutableList.of();
                minTimes = 0;
                olapTable.getBaseIndexId();
                result = 1L;
                minTimes = 0;
                olapTable.getKeysType();
                result = KeysType.DUP_KEYS;
                minTimes = 0;
                olapTable.getBaseSchema(true);
                result = ImmutableList.of(valueColumn);
                minTimes = 0;
                olapTable.getName();
                result = "dup_tbl";
                minTimes = 0;
            }
        };

        LogicalOlapScan scan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), olapTable,
                ImmutableList.of("ctl", "db"));
        Plan rewritten = new IVMCreateSqlRewriter().rewriteForCreate(scan);

        Assertions.assertInstanceOf(LogicalProject.class, rewritten);
        NamedExpression rowIdExpr = ((LogicalProject<?>) rewritten).getProjects().get(1);
        Assertions.assertInstanceOf(Alias.class, rowIdExpr);
        Assertions.assertInstanceOf(Uuid.class, ((Alias) rowIdExpr).child());
    }

    @Test
    public void testRewriteForCreateRejectsUnsupportedKeyType(@Mocked OlapTable olapTable) {
        new Expectations() {
            {
                olapTable.getPartitionIds();
                result = ImmutableList.of();
                minTimes = 0;
                olapTable.getBaseIndexId();
                result = 1L;
                minTimes = 0;
                olapTable.getKeysType();
                result = KeysType.AGG_KEYS;
                minTimes = 0;
                olapTable.getName();
                result = "agg_tbl";
                minTimes = 0;
            }
        };

        LogicalOlapScan scan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), olapTable,
                ImmutableList.of("ctl", "db"));
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> new IVMCreateSqlRewriter().rewriteForCreate(scan));
        Assertions.assertTrue(exception.getMessage().contains("does not support binlog and stream"));
    }

    @Test
    public void testRewriteForCreateRejectsExternalBaseTable(@Mocked ExternalTable externalTable) {
        LogicalFileScan.SelectedPartitions selectedPartitions =
                new LogicalFileScan.SelectedPartitions(1, ImmutableMap.of(), false);
        new Expectations() {
            {
                externalTable.initSelectedPartitions((Optional) any);
                result = selectedPartitions;
                minTimes = 1;
            }
        };

        LogicalFileScan scan = new LogicalFileScan(StatementScopeIdGenerator.newRelationId(), externalTable,
                ImmutableList.of("ctl", "db"), ImmutableList.of(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> new IVMCreateSqlRewriter().rewriteForCreate(scan));
        Assertions.assertTrue(exception.getMessage().contains("External base table does not support IVM rewrite"));
    }
}
