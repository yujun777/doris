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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Delta planner for aggregate patterns: {@link IVMPlanPattern#AGG_ON_SCAN}
 * and {@link IVMPlanPattern#AGG_ON_INNER_JOIN}.
 *
 * <p>For root aggregate MVs, the delta propagation rule is:
 * <ol>
 *   <li>Replace the driving table scan with a stream TVF (done by parent)</li>
 *   <li>Extract the root {@link LogicalAggregate} from the delta plan</li>
 *   <li>Rewrite to a group-level delta plan (identity for now — the aggregate
 *       over the stream TVF input naturally produces group-level deltas)</li>
 *   <li>Validate retraction safety: if the stream supports deletes and the
 *       aggregate contains {@code min/max}, fallback is required because
 *       removing the current min/max value cannot be resolved locally</li>
 *   <li>Build write-back plans using MERGE (keyed by group-by columns)</li>
 * </ol>
 *
 * <p>Design notes:
 * <ul>
 *   <li>{@code avg} is rewritten to {@code sum/count} at MV creation time,
 *       so the planner only sees {@code sum} and {@code count} at runtime</li>
 *   <li>{@code min/max} with delete/update triggers fallback to full refresh</li>
 * </ul>
 */
public class IVMAggDeltaPlanner extends AbstractNereidsIVMDeltaPlanner {

    public IVMAggDeltaPlanner(
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

        // Step 2: extract the root aggregate node
        LogicalAggregate<? extends Plan> rootAgg = extractRootAggregate(baseDeltaPlan);

        // Step 3: rewrite to group-level delta
        Plan groupDeltaPlan = rewriteToGroupDelta(baseDeltaPlan, rootAgg);

        // Step 4: validate retraction safety (min/max with deletes)
        validateRetraction(groupDeltaPlan, rootAgg, context, baseDeltaSnapshot, mtmv);

        // Step 5: build write-back plans
        return buildAggWritePlans(mtmv, groupDeltaPlan, rootAgg, context, baseDeltaSnapshot);
    }

    /**
     * Walks down through optional {@link LogicalProject} and {@link LogicalFilter}
     * nodes to find the root {@link LogicalAggregate}.
     *
     * @throws AnalysisException if no aggregate node is found (should not happen
     *         if the plan analyzer classified the pattern correctly)
     */
    protected LogicalAggregate<? extends Plan> extractRootAggregate(
            Plan plan) throws AnalysisException {
        Plan current = plan;
        while (current != null) {
            if (current instanceof LogicalAggregate) {
                return (LogicalAggregate<? extends Plan>) current;
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
                "No LogicalAggregate found in delta plan. "
                + "Plan structure does not match AGG_ON_SCAN/AGG_ON_INNER_JOIN pattern: " + plan);
    }

    /**
     * Rewrites the base delta plan to a group-level delta plan.
     *
     * <p>For root aggregates, this is currently an identity transformation:
     * the aggregate over the stream TVF input naturally produces group-level
     * deltas (one row per affected group). Future work may add explicit
     * deduplication or partial aggregation rewriting here.
     */
    protected Plan rewriteToGroupDelta(
            Plan baseDeltaPlan,
            LogicalAggregate<? extends Plan> rootAgg) {
        return baseDeltaPlan;
    }

    /**
     * Validates that retraction is safe for the current aggregate.
     *
     * <p>If the stream supports deletes and the aggregate contains
     * {@code min} or {@code max} functions, incremental maintenance cannot
     * guarantee correctness — removing the current min/max value would
     * require a full re-scan to find the new min/max. In this case,
     * an {@link AnalysisException} is thrown to trigger fallback.
     */
    protected void validateRetraction(
            Plan groupDeltaPlan,
            LogicalAggregate<? extends Plan> rootAgg,
            IVMRefreshContext context,
            BaseDeltaSnapshot baseDeltaSnapshot,
            MTMV mtmv) throws AnalysisException {
        if (containsUnsupportedMinMaxRetraction(rootAgg, baseDeltaSnapshot, mtmv)) {
            throw new AnalysisException(
                    "min/max aggregate with delete-capable stream requires fallback "
                    + "to full refresh: incremental retraction is not supported");
        }
    }

    /**
     * Checks whether the aggregate contains {@code min} or {@code max} functions
     * and the driving table's stream supports deletes. This combination is
     * unsafe for incremental maintenance because deleting the current extremum
     * requires a full re-scan.
     */
    protected boolean containsUnsupportedMinMaxRetraction(
            LogicalAggregate<? extends Plan> rootAgg,
            BaseDeltaSnapshot baseDeltaSnapshot,
            MTMV mtmv) throws AnalysisException {
        // Check if the stream supports deletes
        StreamSubscription subscription = openSubscription(
                mtmv, baseDeltaSnapshot.getDrivingTable());
        StreamCapability capability = subscription.getStream().getCapability();
        if (!capability.isSupportsDelete()) {
            // Append-only stream: min/max is safe since values are never removed
            return false;
        }

        // Check if aggregate contains min or max
        Set<AggregateFunction> aggFunctions = collectAggregateFunctions(rootAgg);
        for (AggregateFunction func : aggFunctions) {
            if (func instanceof Min || func instanceof Max) {
                return true;
            }
        }
        return false;
    }

    /**
     * Collects all {@link AggregateFunction} instances from the aggregate's
     * output expressions.
     */
    private Set<AggregateFunction> collectAggregateFunctions(
            LogicalAggregate<? extends Plan> rootAgg) {
        return rootAgg.getOutputExpressions().stream()
                .flatMap(expr -> extractAggregateFunctions(expr).stream())
                .collect(Collectors.toSet());
    }

    /**
     * Recursively extracts {@link AggregateFunction} instances from an expression tree.
     */
    private List<AggregateFunction> extractAggregateFunctions(Expression expr) {
        List<AggregateFunction> result = new ArrayList<>();
        if (expr instanceof AggregateFunction) {
            result.add((AggregateFunction) expr);
        }
        for (Expression child : expr.children()) {
            result.addAll(extractAggregateFunctions(child));
        }
        return result;
    }

    /**
     * Builds write-back plans for aggregate delta.
     *
     * <p>For aggregates, the write-back is a MERGE keyed by group-by columns:
     * matched groups get their aggregate values updated, unmatched groups get
     * inserted. If the stream supports deletes, a DELETE plan is also generated
     * for groups that become empty.
     */
    protected List<Plan> buildAggWritePlans(
            MTMV mtmv,
            Plan groupDeltaPlan,
            LogicalAggregate<? extends Plan> rootAgg,
            IVMRefreshContext context,
            BaseDeltaSnapshot baseDeltaSnapshot) throws AnalysisException {
        List<Plan> writePlans = new ArrayList<>();

        // For aggregates, use MERGE to upsert group-level results
        writePlans.add(commandBuilder.buildMergePlan(mtmv, groupDeltaPlan));

        // If the stream supports deletes, generate a DELETE plan for
        // groups that may become empty after retraction
        StreamSubscription subscription = openSubscription(
                mtmv, baseDeltaSnapshot.getDrivingTable());
        StreamCapability capability = subscription.getStream().getCapability();
        if (capability.isSupportsDelete()) {
            writePlans.add(commandBuilder.buildDeletePlan(mtmv, groupDeltaPlan));
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
