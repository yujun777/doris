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
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;

import java.util.ArrayList;
import java.util.List;

/**
 * Delta planner for union patterns: {@link IVMPlanPattern#UNION_ALL_ROOT}.
 *
 * <p>For root UNION ALL MVs, delta propagation is straightforward:
 * only the child branch containing the driving table produces delta rows.
 * The parent class already replaces the driving table scan with a stream TVF
 * and binds other tables to their snapshots. The union planner then generates
 * write-back plans for the affected branch.
 *
 * <p>Constraints:
 * <ul>
 *   <li>Only {@code UNION ALL} is supported; {@code UNION DISTINCT}
 *       is rejected at plan analysis time</li>
 *   <li>Only one child branch should be affected by the current
 *       driving table per bundle</li>
 * </ul>
 */
public class IVMUnionDeltaPlanner extends AbstractNereidsIVMDeltaPlanner {

    public IVMUnionDeltaPlanner(
            IVMBaseScanRewriter scanRewriter,
            IVMDeltaCommandBuilder commandBuilder) {
        super(scanRewriter, commandBuilder);
    }

    @Override
    protected List<Plan> generateMergePlans(
            MTMV mtmv,
            IVMRefreshContext context,
            BaseDeltaSnapshot baseDeltaSnapshot) throws AnalysisException {
        // Step 1: build the delta read plan (driving scan replaced with stream TVF,
        //         other tables bound to snapshots)
        Plan baseDeltaPlan = buildBaseDeltaPlan(
                context.getRewrittenMvPlan(), baseDeltaSnapshot);

        // Step 2: extract and validate the root union node
        LogicalUnion rootUnion = extractRootUnion(baseDeltaPlan);

        // Step 3: apply union-specific delta rewriting
        Plan unionDeltaPlan = rewriteToUnionDelta(baseDeltaPlan, rootUnion, baseDeltaSnapshot);

        // Step 4: build write-back plans
        return buildUnionWritePlans(mtmv, unionDeltaPlan, context, baseDeltaSnapshot);
    }

    /**
     * Walks down through optional {@link LogicalProject} and {@link LogicalFilter}
     * nodes to find the root {@link LogicalUnion}.
     *
     * <p>Also validates that the union uses {@code ALL} qualifier.
     * {@code UNION DISTINCT} should have been rejected at plan analysis time,
     * but this provides a safety check.
     *
     * @throws AnalysisException if no union node is found or if the qualifier
     *         is not {@code ALL}
     */
    protected LogicalUnion extractRootUnion(Plan plan) throws AnalysisException {
        Plan current = plan;
        while (current != null) {
            if (current instanceof LogicalUnion) {
                LogicalUnion union = (LogicalUnion) current;
                if (union.getQualifier() != Qualifier.ALL) {
                    throw new AnalysisException(
                            "Only UNION ALL is supported for incremental refresh, "
                            + "but found: UNION " + union.getQualifier());
                }
                return union;
            }
            if (current instanceof LogicalProject || current instanceof LogicalFilter) {
                if (current.children().isEmpty()) {
                    break;
                }
                current = current.child(0);
            } else {
                break;
            }
        }
        throw new AnalysisException(
                "No LogicalUnion found in delta plan. "
                + "Plan structure does not match UNION_ALL_ROOT pattern: " + plan);
    }

    /**
     * Applies union-specific delta rewriting.
     *
     * <p>For UNION ALL, this is an identity transformation — the parent class
     * has already replaced the driving table scan with a stream TVF in the
     * affected branch. Only the branch containing the driving table produces
     * delta rows; other branches contribute no changes.
     */
    protected Plan rewriteToUnionDelta(
            Plan baseDeltaPlan,
            LogicalUnion rootUnion,
            BaseDeltaSnapshot baseDeltaSnapshot) {
        return baseDeltaPlan;
    }

    /**
     * Builds the INSERT and optional DELETE write-back plans for union delta.
     *
     * <p>For UNION ALL, the write-back follows the same pattern as scan:
     * INSERT for new rows, DELETE if the stream supports deletes.
     */
    protected List<Plan> buildUnionWritePlans(
            MTMV mtmv,
            Plan unionDeltaPlan,
            IVMRefreshContext context,
            BaseDeltaSnapshot baseDeltaSnapshot) throws AnalysisException {
        List<Plan> writePlans = new ArrayList<>();

        // Always generate an INSERT plan for new/changed rows
        writePlans.add(commandBuilder.buildInsertPlan(mtmv, unionDeltaPlan));

        // If the stream supports deletes, generate a DELETE plan
        StreamSubscription subscription = openSubscription(
                mtmv, baseDeltaSnapshot.getDrivingTable());
        StreamCapability capability = subscription.getStream().getCapability();
        if (capability.isSupportsDelete()) {
            writePlans.add(commandBuilder.buildDeletePlan(mtmv, unionDeltaPlan));
        }

        return writePlans;
    }

    @Override
    protected StreamSubscription openSubscription(IVMStreamRef streamRef) throws AnalysisException {
        // TODO: implement when stream layer is ready
        throw new AnalysisException(
                "Stream subscription not yet implemented for: " + streamRef);
    }
}
