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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.SupportTableSnapshot;
import org.apache.doris.nereids.util.PlanUtils;

import java.util.Map;
import java.util.Set;

/**
 * Checks whether incremental refresh is viable for a materialized view.
 *
 * <p>Performs a series of checks:
 * <ol>
 *   <li>Previous run must have completed (no lingering in-progress state)</li>
 *   <li>Binlog must not be broken</li>
 *   <li>Plan pattern must be supported</li>
 *   <li>All base tables must have stream bindings</li>
 * </ol>
 */
public class IVMCapabilityChecker {

    /**
     * Checks whether the given MV can be incrementally refreshed in the
     * current context.
     *
     * @param mtmv the materialized view
     * @param context the IVM refresh context (must contain a valid plan analysis)
     * @return result indicating whether incremental refresh is viable,
     *         with a fallback reason if not
     */
    public IVMCapabilityResult check(MTMV mtmv, IVMRefreshContext context) {
        IVMInfo ivmInfo = mtmv.getIvmInfo();

        // Check 1: previous run must have completed
        if (ivmInfo.isInIncrementalRefresh()) {
            return IVMCapabilityResult.unsupported(
                    FallbackReason.PREVIOUS_RUN_INCOMPLETE,
                    "Previous incremental refresh did not complete");
        }

        // Check 2: binlog must not be broken
        if (ivmInfo.isBinlogBroken()) {
            return IVMCapabilityResult.unsupported(
                    FallbackReason.BINLOG_BROKEN,
                    "Stream binlog is marked as broken");
        }

        // Check 3: plan pattern must be supported
        IVMPlanAnalysis analysis = context.getPlanAnalysis();
        if (analysis == null || analysis.getPattern() == IVMPlanPattern.UNSUPPORTED) {
            return IVMCapabilityResult.unsupported(
                    FallbackReason.PLAN_PATTERN_UNSUPPORTED,
                    analysis != null ? analysis.getUnsupportedReason()
                            : "Plan analysis not available");
        }

        // Check 4: all base tables must have stream bindings
        Map<BaseTableId, IVMStreamRef> baseTableStreams = ivmInfo.getBaseTableStreams();
        if (baseTableStreams == null) {
            return IVMCapabilityResult.unsupported(
                    FallbackReason.STREAM_UNSUPPORTED,
                    "No stream bindings are registered for this materialized view");
        }
        for (BaseTableId tableId : context.getBaseTableOrder()) {
            IVMStreamRef streamRef = baseTableStreams.get(tableId);
            if (streamRef == null) {
                return IVMCapabilityResult.unsupported(
                        FallbackReason.STREAM_UNSUPPORTED,
                        "No stream binding found for base table: " + tableId);
            }
            if (streamRef.getStreamType() != StreamType.OLAP) {
                return IVMCapabilityResult.unsupported(
                        FallbackReason.STREAM_UNSUPPORTED,
                        "Only OLAP base table streams are supported for incremental refresh: " + tableId);
            }
            try {
                TableIf table = MTMVUtil.getTable(tableId.getTableInfo());
                if (!(table instanceof OlapTable)) {
                    return IVMCapabilityResult.unsupported(
                            FallbackReason.STREAM_UNSUPPORTED,
                            "Only OLAP base tables are supported for incremental refresh: " + tableId);
                }
            } catch (AnalysisException e) {
                return IVMCapabilityResult.unsupported(
                        FallbackReason.STREAM_UNSUPPORTED,
                        "Failed to resolve base table metadata for incremental refresh: "
                                + tableId + ", reason=" + e.getMessage());
            }
        }

        // Check 5: multi-base refresh requires every base scan to support snapshot rebinding or MVCC snapshots
        if (context.getBaseTableOrder().size() > 1) {
            IVMCapabilityResult snapshotBindingCapability = checkSnapshotBindingCapability(context);
            if (!snapshotBindingCapability.isIncremental()) {
                return snapshotBindingCapability;
            }
        }

        return IVMCapabilityResult.ok();
    }

    private IVMCapabilityResult checkSnapshotBindingCapability(IVMRefreshContext context) {
        if (!(context.getRewrittenMvPlan() instanceof LogicalPlan)) {
            return IVMCapabilityResult.unsupported(
                    FallbackReason.SNAPSHOT_ALIGNMENT_UNSUPPORTED,
                    "Rewritten MV plan is not a logical plan");
        }
        Set<LogicalCatalogRelation> scans = PlanUtils.getLogicalScanFromRootPlan(
                (LogicalPlan) context.getRewrittenMvPlan());
        for (BaseTableId tableId : context.getBaseTableOrder()) {
            LogicalCatalogRelation relation = findRelation(scans, tableId);
            if (relation == null) {
                return IVMCapabilityResult.unsupported(
                        FallbackReason.SNAPSHOT_ALIGNMENT_UNSUPPORTED,
                        "Unable to find scan node for base table: " + tableId);
            }
            if (relation instanceof SupportTableSnapshot || relation.getTable() instanceof MvccTable) {
                continue;
            }
            return IVMCapabilityResult.unsupported(
                    FallbackReason.SNAPSHOT_ALIGNMENT_UNSUPPORTED,
                    "Base table scan does not support snapshot rebinding: " + tableId
                            + ", relation=" + relation.getClass().getSimpleName());
        }
        return IVMCapabilityResult.ok();
    }

    private LogicalCatalogRelation findRelation(Set<LogicalCatalogRelation> scans, BaseTableId tableId) {
        for (LogicalCatalogRelation relation : scans) {
            if (matchesTable(relation, tableId)) {
                return relation;
            }
        }
        return null;
    }

    private boolean matchesTable(LogicalCatalogRelation relation, BaseTableId targetTableId) {
        String ctlName = relation.getQualifier().size() >= 1 ? relation.getQualifier().get(0) : "";
        String dbName = relation.getQualifier().size() >= 2 ? relation.getQualifier().get(1) : "";
        return relation.getTable().getName().equalsIgnoreCase(targetTableId.getTableInfo().getTableName())
                && dbName.equalsIgnoreCase(targetTableId.getTableInfo().getDbName())
                && ctlName.equalsIgnoreCase(targetTableId.getTableInfo().getCtlName());
    }
}
