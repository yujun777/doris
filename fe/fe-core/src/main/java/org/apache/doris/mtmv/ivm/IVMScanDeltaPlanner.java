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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.ArrayList;
import java.util.List;

/**
 * Delta planner for scan-only patterns: {@link IVMPlanPattern#SCAN_ONLY}
 * and {@link IVMPlanPattern#FILTER_PROJECT_SCAN}.
 *
 * <p>For single-table MVs, the delta plan is straightforward:
 * replace the base table scan with a stream TVF relation, then generate
 * INSERT (and optionally DELETE) write-back plans targeting the MV.
 *
 * <p>The stream capability determines which write-back plans are generated:
 * <ul>
 *   <li>Always: INSERT plan for new/changed rows</li>
 *   <li>If stream supports deletes: DELETE plan for removed rows</li>
 * </ul>
 */
public class IVMScanDeltaPlanner extends AbstractNereidsIVMDeltaPlanner {

    public IVMScanDeltaPlanner(
            IVMBaseScanRewriter scanRewriter,
            IVMDeltaCommandBuilder commandBuilder) {
        super(scanRewriter, commandBuilder);
    }

    @Override
    protected List<Plan> generateMergePlans(
            MTMV mtmv,
            IVMRefreshContext context,
            BaseDeltaSnapshot baseDeltaSnapshot) throws AnalysisException {
        // Step 1: build the delta read plan (scan replaced with stream TVF)
        Plan baseDeltaPlan = buildBaseDeltaPlan(
                context.getRewrittenMvPlan(), baseDeltaSnapshot);

        // Step 2: normalize row delta (identity for scan pattern)
        Plan normalizedDeltaPlan = normalizeRowDelta(baseDeltaPlan, context);

        // Step 3: build write-back plans
        return buildRowDeltaWritePlans(mtmv, normalizedDeltaPlan, context, baseDeltaSnapshot);
    }

    /**
     * Normalizes the delta plan for row-level changes.
     * For scan patterns, this is an identity transformation.
     * Subclasses for join/agg patterns will override this to apply
     * pattern-specific normalization (e.g., deduplication, retraction).
     */
    protected Plan normalizeRowDelta(Plan baseDeltaPlan, IVMRefreshContext context) {
        return baseDeltaPlan;
    }

    /**
     * Builds the INSERT and optional DELETE write-back plans.
     *
     * <p>For append-only streams, only an INSERT plan is generated.
     * For streams that support deletes, both INSERT and DELETE plans
     * are generated. For MOW unique key tables, a single MERGE plan
     * may be used instead.
     */
    protected List<Plan> buildRowDeltaWritePlans(
            MTMV mtmv,
            Plan normalizedDeltaPlan,
            IVMRefreshContext context,
            BaseDeltaSnapshot baseDeltaSnapshot) throws AnalysisException {
        List<Plan> writePlans = new ArrayList<>();

        // Always generate an INSERT plan for new/changed rows
        writePlans.add(commandBuilder.buildInsertPlan(mtmv, normalizedDeltaPlan));

        // If the stream supports deletes, generate a DELETE plan
        StreamSubscription subscription = getSubscriptionForTable(
                mtmv, baseDeltaSnapshot.getDrivingTable());
        StreamCapability capability = subscription.getStream().getCapability();
        if (capability.isSupportsDelete()) {
            writePlans.add(commandBuilder.buildDeletePlan(mtmv, normalizedDeltaPlan));
        }

        return writePlans;
    }

    /**
     * Retrieves the subscription for the driving table.
     * This re-opens the subscription, which should be idempotent.
     */
    private StreamSubscription getSubscriptionForTable(MTMV mtmv, BaseTableId tableId)
            throws AnalysisException {
        return openSubscription(mtmv, tableId);
    }

    @Override
    protected StreamSubscription openSubscription(IVMStreamRef streamRef) throws AnalysisException {
        // TODO: implement when stream layer is ready
        throw new AnalysisException(
                "Stream subscription not yet implemented for: " + streamRef);
    }
}
