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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Abstract base class for incremental view maintenance delta planners.
 *
 * <p>The {@link #plan} template method iterates over base tables in order,
 * opens runtime {@link StreamSubscription}s, and for each driving table
 * that has binlog, builds a {@link BaseDeltaSnapshot} and delegates to
 * {@link #generateMergePlans} for pattern-specific write-back plan generation.
 *
 * <p>Subclass hierarchy:
 * <pre>
 * IVMDeltaPlanner (this class)
 *   └─ AbstractIVMDeltaPlanner
 *        └─ IVMScanDeltaPlanner
 *        └─ IVMJoinDeltaPlanner
 *        └─ IVMAggDeltaPlanner
 *        └─ IVMUnionDeltaPlanner
 * </pre>
 */
public abstract class IVMDeltaPlanner {

    /**
     * Template method: iterates all base tables, opens subscriptions, and
     * produces a list of {@link DeltaPlanBundle} for tables that have binlog.
     *
     * @param mtmv the materialized view
     * @param context the IVM refresh context
     * @return list of delta plan bundles, one per driving table with changes
     */
    public final List<DeltaPlanBundle> plan(MTMV mtmv, IVMRefreshContext context)
            throws AnalysisException {
        List<DeltaPlanBundle> bundles = new ArrayList<>();
        Map<BaseTableId, StreamSubscription> runtimeSubscriptions = new HashMap<>();
        Set<BaseTableId> processedTables = new HashSet<>();

        for (BaseTableId tableId : context.getBaseTableOrder()) {
            StreamSubscription subscription = runtimeSubscriptions.computeIfAbsent(
                    tableId,
                    id -> openSubscriptionUnchecked(mtmv, id));

            if (!hasBinlog(subscription)) {
                continue;
            }

            BaseDeltaSnapshot baseDeltaSnapshot = buildBaseDeltaSnapshot(
                    mtmv, context, tableId, subscription,
                    runtimeSubscriptions, processedTables);

            List<Plan> mergePlans = generateMergePlans(mtmv, context, baseDeltaSnapshot);
            bundles.add(new DeltaPlanBundle(
                    tableId, subscription,
                    baseDeltaSnapshot.getTableSnapshots(),
                    mergePlans));

            processedTables.add(tableId);
        }
        for (BaseTableId tableId : context.getBaseTableOrder()) {
            StreamSubscription subscription = runtimeSubscriptions.computeIfAbsent(
                    tableId,
                    id -> openSubscriptionUnchecked(mtmv, id));
            context.recordTargetTableSnapshot(tableId,
                    resolveSnapshot(tableId, subscription.getReadableCursor(), subscription));
        }
        return bundles;
    }

    /**
     * Builds the base delta plan by replacing the driving table's scan with
     * a stream relation and binding other base tables to their snapshots.
     */
    protected final Plan buildBaseDeltaPlan(Plan mvPlan, BaseDeltaSnapshot baseDeltaSnapshot)
            throws AnalysisException {
        Plan replacedPlan = replaceDrivingTableWithStream(
                mvPlan,
                baseDeltaSnapshot.getDrivingTable(),
                baseDeltaSnapshot.getRelationSpec());
        return bindBaseTableSnapshots(replacedPlan, baseDeltaSnapshot.getTableSnapshots());
    }

    /**
     * Builds a {@link BaseDeltaSnapshot} for one driving table, resolving
     * the correct before/after snapshots for all other base tables.
     */
    protected BaseDeltaSnapshot buildBaseDeltaSnapshot(
            MTMV mtmv,
            IVMRefreshContext context,
            BaseTableId drivingTable,
            StreamSubscription subscription,
            Map<BaseTableId, StreamSubscription> runtimeSubscriptions,
            Set<BaseTableId> processedTables) throws AnalysisException {
        Map<BaseTableId, IVMTableSnapshot> tableSnapshots = resolveTableSnapshots(
                mtmv, context, drivingTable,
                runtimeSubscriptions, processedTables);

        StreamRelationSpec relationSpec = subscription.getStream().createRelationSpec(
                subscription.getCommittedCursor(),
                subscription.getReadableCursor());

        return new BaseDeltaSnapshot(drivingTable, relationSpec, tableSnapshots,
                subscription.getStream().getCapability());
    }

    /**
     * Resolves snapshots for all base tables except the driving table.
     * Already-processed tables use afterSnapshot (readableCursor);
     * not-yet-processed tables use beforeSnapshot (committedCursor).
     */
    protected Map<BaseTableId, IVMTableSnapshot> resolveTableSnapshots(
            MTMV mtmv,
            IVMRefreshContext context,
            BaseTableId drivingTable,
            Map<BaseTableId, StreamSubscription> runtimeSubscriptions,
            Set<BaseTableId> processedTables) throws AnalysisException {
        Map<BaseTableId, IVMTableSnapshot> tableSnapshots = new HashMap<>();
        for (BaseTableId tableId : context.getBaseTableOrder()) {
            if (tableId.equals(drivingTable)) {
                continue;
            }
            StreamSubscription sub = runtimeSubscriptions.computeIfAbsent(
                    tableId,
                    id -> openSubscriptionUnchecked(mtmv, id));
            StreamCursor cursor = processedTables.contains(tableId)
                    ? sub.getReadableCursor()
                    : sub.getCommittedCursor();
            tableSnapshots.put(tableId, resolveSnapshot(tableId, cursor, sub));
        }
        return tableSnapshots;
    }

    /**
     * Opens a subscription for a base table using the IVMStreamRef stored in MTMV metadata.
     */
    protected StreamSubscription openSubscription(MTMV mtmv, BaseTableId tableId)
            throws AnalysisException {
        IVMStreamRef streamRef = mtmv.getIvmInfo().getBaseTableStreams().get(tableId);
        if (streamRef == null) {
            throw new AnalysisException("No stream reference found for base table: " + tableId);
        }
        return openSubscription(streamRef);
    }

    /**
     * Opens a subscription from a stream reference.
     * To be implemented when the stream layer is ready.
     */
    protected abstract StreamSubscription openSubscription(IVMStreamRef streamRef)
            throws AnalysisException;

    /**
     * Checks whether the stream has binlog between committed and readable cursors.
     */
    protected boolean hasBinlog(StreamSubscription subscription) throws AnalysisException {
        return subscription.getStream().hasBinlog(
                subscription.getCommittedCursor(),
                subscription.getReadableCursor());
    }

    /**
     * Returns a snapshot of the base table at the given cursor position.
     */
    protected IVMTableSnapshot resolveSnapshot(BaseTableId tableId, StreamCursor cursor,
            StreamSubscription subscription) throws AnalysisException {
        IVMTableSnapshot tableSnapshot = subscription.getStream().snapshotAt(cursor);
        return enrichResolvedSnapshot(tableId, tableSnapshot);
    }

    /**
     * Pattern-specific: generates the write-back plans (INSERT/DELETE/MERGE)
     * from the MV plan and the base delta snapshot.
     */
    protected abstract List<Plan> generateMergePlans(
            MTMV mtmv,
            IVMRefreshContext context,
            BaseDeltaSnapshot baseDeltaSnapshot) throws AnalysisException;

    /**
     * Replaces the driving table's scan node with a stream TVF relation.
     */
    protected abstract Plan replaceDrivingTableWithStream(
            Plan rewrittenMvPlan,
            BaseTableId drivingTable,
            StreamRelationSpec relationSpec);

    /**
     * Binds non-driving base tables to their consistent snapshots.
     */
    protected abstract Plan bindBaseTableSnapshots(
            Plan replacedPlan,
            Map<BaseTableId, IVMTableSnapshot> tableSnapshots) throws AnalysisException;

    /**
     * Wrapper that converts checked AnalysisException to unchecked for use
     * in lambda contexts (e.g., Map.computeIfAbsent).
     */
    private StreamSubscription openSubscriptionUnchecked(MTMV mtmv, BaseTableId tableId) {
        try {
            return openSubscription(mtmv, tableId);
        } catch (AnalysisException e) {
            throw new RuntimeException("Failed to open subscription for " + tableId, e);
        }
    }

    private IVMTableSnapshot enrichResolvedSnapshot(BaseTableId tableId, IVMTableSnapshot tableSnapshot)
            throws AnalysisException {
        TableIf table = MTMVUtil.getTable(tableId.getTableInfo());
        if (!(table instanceof MTMVRelatedTableIf)) {
            return tableSnapshot;
        }
        Optional<MvccSnapshot> mvccSnapshot = tableSnapshot.asMvccSnapshot();
        if (!mvccSnapshot.isPresent() && table instanceof MvccTable && tableSnapshot.asTableSnapshot().isPresent()) {
            mvccSnapshot = Optional.of(((MvccTable) table).loadSnapshot(
                    tableSnapshot.asTableSnapshot(), Optional.empty()));
        }
        Optional<MTMVSnapshotIf> mtmvSnapshot = tableSnapshot.asMtmvSnapshot();
        if (!mtmvSnapshot.isPresent()) {
            mtmvSnapshot = Optional.of(((MTMVRelatedTableIf) table).getTableSnapshot(mvccSnapshot));
        }
        return new IVMVersionedTableSnapshot(tableSnapshot.asTableSnapshot(), mvccSnapshot, mtmvSnapshot);
    }
}
