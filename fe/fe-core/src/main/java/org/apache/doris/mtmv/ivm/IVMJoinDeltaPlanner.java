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
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import java.util.ArrayList;
import java.util.List;

/**
 * Delta planner for inner join patterns: {@link IVMPlanPattern#INNER_JOIN}.
 *
 * <p>For multi-table join MVs (V = S join T), the delta propagation rule is:
 * <ul>
 *   <li>bundle(S): delta(S) join T@before</li>
 *   <li>bundle(T): S@after join delta(T)</li>
 * </ul>
 *
 * <p>The parent class {@link IVMDeltaPlanner#plan} already handles iterating
 * base tables in order, opening subscriptions, building {@link BaseDeltaSnapshot}
 * with correct before/after snapshots per processedTables, and calling
 * {@link #buildBaseDeltaPlan} which replaces the driving table scan with a
 * stream TVF and binds other tables to their snapshots.
 *
 * <p>This planner only needs to:
 * <ol>
 *   <li>Extract the join node from the delta plan</li>
 *   <li>Apply any join-specific rewriting (identity for simple inner join)</li>
 *   <li>Build write-back plans (INSERT + optional DELETE)</li>
 * </ol>
 */
public class IVMJoinDeltaPlanner extends AbstractNereidsIVMDeltaPlanner {

    public IVMJoinDeltaPlanner(
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

        // Step 2: extract and validate the join node
        LogicalJoin<?, ?> rootJoin = extractJoin(baseDeltaPlan);

        // Step 3: apply join-specific delta rewriting
        Plan joinDeltaPlan = rewriteToJoinDelta(baseDeltaPlan, rootJoin, baseDeltaSnapshot);

        // Step 4: build write-back plans
        return buildJoinWritePlans(mtmv, joinDeltaPlan, context, baseDeltaSnapshot);
    }

    /**
     * Walks down through optional {@link LogicalProject} and {@link LogicalFilter}
     * nodes to find the {@link LogicalJoin}.
     *
     * @throws AnalysisException if no join node is found (should not happen if
     *         the plan analyzer classified the pattern correctly)
     */
    protected LogicalJoin<?, ?> extractJoin(Plan plan) throws AnalysisException {
        Plan current = plan;
        while (current != null) {
            if (current instanceof LogicalJoin) {
                return (LogicalJoin<?, ?>) current;
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
                "No LogicalJoin found in delta plan. "
                + "Plan structure does not match INNER_JOIN pattern: " + plan);
    }

    /**
     * Applies join-specific delta rewriting.
     *
     * <p>For simple inner join, this is an identity transformation — the parent
     * class has already replaced the driving table scan with a stream TVF and
     * bound other tables to their correct snapshots. Future work may add
     * anti-join retraction logic here for more complex join patterns.
     */
    protected Plan rewriteToJoinDelta(
            Plan baseDeltaPlan,
            LogicalJoin<?, ?> rootJoin,
            BaseDeltaSnapshot baseDeltaSnapshot) {
        return baseDeltaPlan;
    }

    /**
     * Builds the INSERT and optional DELETE write-back plans for join delta.
     *
     * <p>For append-only streams, only an INSERT plan is generated.
     * For streams that support deletes, both INSERT and DELETE plans are generated.
     */
    protected List<Plan> buildJoinWritePlans(
            MTMV mtmv,
            Plan joinDeltaPlan,
            IVMRefreshContext context,
            BaseDeltaSnapshot baseDeltaSnapshot) throws AnalysisException {
        List<Plan> writePlans = new ArrayList<>();

        // Always generate an INSERT plan for new/changed rows
        writePlans.add(commandBuilder.buildInsertPlan(mtmv, joinDeltaPlan));

        // If the stream supports deletes, generate a DELETE plan
        StreamSubscription subscription = openSubscription(
                mtmv, baseDeltaSnapshot.getDrivingTable());
        StreamCapability capability = subscription.getStream().getCapability();
        if (capability.isSupportsDelete()) {
            writePlans.add(commandBuilder.buildDeletePlan(mtmv, joinDeltaPlan));
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
