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
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVRefreshSnapshot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class IVMCorrectnessTest {

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
    public void testBindBaseTableSnapshotsWritesTableSnapshotToLogicalFileScan(
            @Mocked ExternalTable externalTable,
            @Mocked BaseTableInfo tableInfo) throws Exception {
        LogicalFileScan.SelectedPartitions selectedPartitions =
                new LogicalFileScan.SelectedPartitions(1, ImmutableMap.of(), false);
        new Expectations() {
            {
                externalTable.initSelectedPartitions((Optional) any);
                result = selectedPartitions;
                minTimes = 1;

                externalTable.getName();
                result = "tbl";
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

        LogicalFileScan scan = new LogicalFileScan(StatementScopeIdGenerator.newRelationId(), externalTable,
                ImmutableList.of("ctl", "db"), ImmutableList.of(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
        TableSnapshot tableSnapshot = TableSnapshot.versionOf("101");

        LogicalFileScan rewritten = (LogicalFileScan) new IVMBaseScanRewriter().bindBaseTableSnapshots(scan,
                ImmutableMap.of(new BaseTableId(tableInfo), new IVMVersionedTableSnapshot(
                        Optional.of(tableSnapshot), Optional.empty(), Optional.empty())));
        Assertions.assertTrue(rewritten.getTableSnapshot().isPresent());
        Assertions.assertEquals("101", rewritten.getTableSnapshot().get().getValue());
    }

    @Test
    public void testBindBaseTableSnapshotsRejectsUnsupportedLogicalOlapScan(
            @Mocked OlapTable olapTable,
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
