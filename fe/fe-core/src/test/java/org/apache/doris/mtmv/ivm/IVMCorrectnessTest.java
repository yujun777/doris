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

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVRefreshSnapshot;
import org.apache.doris.mtmv.MTMVUtil;
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
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class IVMCorrectnessTest {

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

    @Test
    public void testCapabilityRejectsMissingStreamBinding(
            @Mocked MTMV mtmv,
            @Mocked BaseTableInfo firstTableInfo,
            @Mocked BaseTableInfo secondTableInfo) {
        BaseTableId firstTable = new BaseTableId(firstTableInfo);
        BaseTableId secondTable = new BaseTableId(secondTableInfo);
        IVMInfo ivmInfo = new IVMInfo();
        Map<BaseTableId, IVMStreamRef> streamBindings = Maps.newHashMap();
        streamBindings.put(firstTable, new IVMStreamRef(StreamType.OLAP, "consumer-1", ImmutableMap.of()));
        ivmInfo.setBaseTableStreams(streamBindings);

        new Expectations() {
            {
                mtmv.getIvmInfo();
                result = ivmInfo;
            }
        };

        IVMRefreshContext context = new IVMRefreshContext(null, null, ImmutableList.of(firstTable, secondTable));
        context.setPlanAnalysis(new IVMPlanAnalysis(IVMPlanPattern.SCAN_ONLY, null));

        IVMCapabilityResult result = new IVMCapabilityChecker().check(mtmv, context);
        Assertions.assertFalse(result.isIncremental());
        Assertions.assertEquals(FallbackReason.STREAM_UNSUPPORTED, result.getFallbackReason());
    }

    @Test
    public void testCapabilityAllowsMultiBaseLogicalOlapScan(
            @Mocked MTMV mtmv,
            @Mocked OlapTable firstTable,
            @Mocked BaseTableInfo firstTableInfo,
            @Mocked BaseTableInfo secondTableInfo) {
        BaseTableId firstTableId = new BaseTableId(firstTableInfo);
        BaseTableId secondTableId = new BaseTableId(secondTableInfo);
        IVMInfo ivmInfo = new IVMInfo();
        Map<BaseTableId, IVMStreamRef> streamBindings = Maps.newHashMap();
        streamBindings.put(firstTableId, new IVMStreamRef(StreamType.OLAP, "consumer-1", ImmutableMap.of()));
        streamBindings.put(secondTableId, new IVMStreamRef(StreamType.OLAP, "consumer-2", ImmutableMap.of()));
        ivmInfo.setBaseTableStreams(streamBindings);

        new Expectations() {
            {
                mtmv.getIvmInfo();
                result = ivmInfo;

                firstTable.getPartitionIds();
                result = ImmutableList.of();
                minTimes = 0;
                firstTable.getBaseIndexId();
                result = 1L;
                minTimes = 0;
                firstTable.getName();
                result = "t1";
                minTimes = 0;

                firstTableInfo.getCtlName();
                result = "ctl";
                minTimes = 0;
                firstTableInfo.getDbName();
                result = "db";
                minTimes = 0;
                firstTableInfo.getTableName();
                result = "t1";
                minTimes = 0;
            }
        };
        new MockUp<MTMVUtil>() {
            @Mock
            public TableIf getTable(BaseTableInfo input) {
                return firstTable;
            }
        };

        LogicalOlapScan plan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), firstTable,
                ImmutableList.of("ctl", "db"));
        IVMRefreshContext context = new IVMRefreshContext(null, plan, ImmutableList.of(firstTableId, secondTableId));
        context.setPlanAnalysis(new IVMPlanAnalysis(IVMPlanPattern.INNER_JOIN, null));

        IVMCapabilityResult result = new IVMCapabilityChecker().check(mtmv, context);
        Assertions.assertTrue(result.isIncremental());
    }

    @Test
    public void testCapabilityRejectsExternalBaseTable(
            @Mocked MTMV mtmv,
            @Mocked ExternalTable externalTable,
            @Mocked BaseTableInfo tableInfo) {
        BaseTableId tableId = new BaseTableId(tableInfo);
        IVMInfo ivmInfo = new IVMInfo();
        ivmInfo.setBaseTableStreams(ImmutableMap.of(
                tableId, new IVMStreamRef(StreamType.PAIMON, "consumer-1", ImmutableMap.of())));

        new Expectations() {
            {
                mtmv.getIvmInfo();
                result = ivmInfo;
            }
        };
        new MockUp<MTMVUtil>() {
            @Mock
            public TableIf getTable(BaseTableInfo input) {
                return externalTable;
            }
        };

        IVMRefreshContext context = new IVMRefreshContext(null, null, ImmutableList.of(tableId));
        context.setPlanAnalysis(new IVMPlanAnalysis(IVMPlanPattern.SCAN_ONLY, null));

        IVMCapabilityResult result = new IVMCapabilityChecker().check(mtmv, context);
        Assertions.assertFalse(result.isIncremental());
        Assertions.assertEquals(FallbackReason.STREAM_UNSUPPORTED, result.getFallbackReason());
    }

    @Test
    public void testCapabilityAllowsSingleOlapBaseTable(
            @Mocked MTMV mtmv,
            @Mocked OlapTable olapTable,
            @Mocked BaseTableInfo tableInfo) {
        BaseTableId tableId = new BaseTableId(tableInfo);
        IVMInfo ivmInfo = new IVMInfo();
        ivmInfo.setBaseTableStreams(ImmutableMap.of(
                tableId, new IVMStreamRef(StreamType.OLAP, "consumer-1", ImmutableMap.of())));

        new Expectations() {
            {
                mtmv.getIvmInfo();
                result = ivmInfo;
            }
        };
        new MockUp<MTMVUtil>() {
            @Mock
            public TableIf getTable(BaseTableInfo input) {
                return olapTable;
            }
        };

        IVMRefreshContext context = new IVMRefreshContext(null, null, ImmutableList.of(tableId));
        context.setPlanAnalysis(new IVMPlanAnalysis(IVMPlanPattern.SCAN_ONLY, null));

        IVMCapabilityResult result = new IVMCapabilityChecker().check(mtmv, context);
        Assertions.assertTrue(result.isIncremental());
    }

    @Test
    public void testRefreshContextDoesNotClearExistingSnapshotWhenNoTargetSnapshot(@Mocked MTMV mtmv) throws Exception {
        MTMVRefreshSnapshot currentSnapshot = new MTMVRefreshSnapshot();
        MTMVRefreshContext baseContext = new MTMVRefreshContext(mtmv);

        new Expectations() {
            {
                mtmv.getRefreshSnapshot();
                result = currentSnapshot;
            }
        };

        IVMRefreshContext context = new IVMRefreshContext(baseContext, null, Collections.emptyList());
        Assertions.assertSame(currentSnapshot, context.toRefreshSnapshot());
    }

    @Test
    public void testAppendOnlyValidationRejectsDeleteCapableStream() {
        TestingPlanner planner = new TestingPlanner();
        BaseDeltaSnapshot snapshot = new BaseDeltaSnapshot(
                null,
                null,
                Collections.emptyMap(),
                new StreamCapability(true, true, false, true, true, false));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> planner.assertAppendOnly(snapshot));
        Assertions.assertTrue(exception.getMessage().contains("append-only"));
    }

    @Test
    public void testAppendOnlyValidationAllowsAppendOnlyStream() throws Exception {
        TestingPlanner planner = new TestingPlanner();
        BaseDeltaSnapshot snapshot = new BaseDeltaSnapshot(
                null,
                null,
                Collections.emptyMap(),
                new StreamCapability(false, false, false, true, true, true));

        planner.assertAppendOnly(snapshot);
    }

    @Test
    public void testBindBaseTableSnapshotsRejectsUnsupportedLogicalOlapScan(
            @Mocked OlapTable olapTable,
            @Mocked DatabaseIf database,
            @Mocked CatalogIf catalog,
            @Mocked BaseTableInfo tableInfo) {
        new Expectations() {
            {
                olapTable.getPartitionIds();
                result = ImmutableList.of();
                minTimes = 0;
                olapTable.getBaseIndexId();
                result = 1L;
                minTimes = 0;
                olapTable.getName();
                result = "tbl";
                minTimes = 0;
                olapTable.getDatabase();
                result = database;
                minTimes = 0;
                database.getCatalog();
                result = catalog;
                minTimes = 0;
                catalog.getName();
                result = "ctl";
                minTimes = 0;
                database.getFullName();
                result = "db";
                minTimes = 0;

                tableInfo.getCtlName();
                result = "ctl";
                minTimes = 0;
                tableInfo.getDbName();
                result = "db";
                minTimes = 0;
                tableInfo.getTableName();
                result = "tbl";
                minTimes = 0;
            }
        };

        LogicalOlapScan scan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), olapTable,
                ImmutableList.of("ctl", "db"));
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> new IVMBaseScanRewriter().bindBaseTableSnapshots(scan,
                        ImmutableMap.of(new BaseTableId(tableInfo), new IVMVersionedTableSnapshot(
                                Optional.of(TableSnapshot.versionOf("33")), Optional.empty(), Optional.empty()))));
        Assertions.assertTrue(exception.getMessage().contains("does not support snapshot binding"));
    }

    private static class TestingPlanner extends AbstractIVMDeltaPlanner {
        TestingPlanner() {
            super(new IVMBaseScanRewriter(), new IVMDeltaCommandBuilder());
        }

        void assertAppendOnly(BaseDeltaSnapshot baseDeltaSnapshot) throws AnalysisException {
            validateAppendOnlyStream(baseDeltaSnapshot);
        }

        @Override
        protected StreamSubscription openSubscription(IVMStreamRef streamRef) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected List<Plan> generateMergePlans(MTMV mtmv, IVMRefreshContext context,
                BaseDeltaSnapshot baseDeltaSnapshot) {
            throw new UnsupportedOperationException();
        }
    }
}
