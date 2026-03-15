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

import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;

/**
 * Default implementation of {@link IVMPlanAnalyzer}.
 *
 * <p>Walks the plan tree top-down and classifies it into one of the
 * supported {@link IVMPlanPattern} values. Currently supports:
 * <ul>
 *   <li>{@code SCAN_ONLY} - bare table scan</li>
 *   <li>{@code FILTER_PROJECT_SCAN} - optional filter/project on a single scan</li>
 *   <li>{@code INNER_JOIN} - inner/cross join with optional filter/project</li>
 *   <li>{@code AGG_ON_SCAN} - root aggregate on a single scan</li>
 *   <li>{@code AGG_ON_INNER_JOIN} - root aggregate on an inner/cross join</li>
 *   <li>{@code UNION_ALL_ROOT} - root UNION ALL</li>
 * </ul>
 */
public class IVMPlanAnalyzerImpl implements IVMPlanAnalyzer {

    @Override
    public IVMPlanAnalysis analyze(Plan mvPlan) {
        Plan current = mvPlan;
        boolean hadProject = false;

        // Step 1: strip outer LogicalProject
        if (current instanceof LogicalProject) {
            LogicalProject<?> project = (LogicalProject<?>) current;
            if (project.isDistinct()) {
                return unsupported("SELECT DISTINCT is not supported for IVM");
            }
            hadProject = true;
            current = project.child(0);
        }

        // Step 2: dispatch on core node type

        // --- Filter (with optional inner project) on scan ---
        if (current instanceof LogicalFilter) {
            return analyzeFilter((LogicalFilter<?>) current);
        }

        // --- Bare scan ---
        if (current instanceof LogicalCatalogRelation) {
            return hadProject
                    ? new IVMPlanAnalysis(IVMPlanPattern.FILTER_PROJECT_SCAN, null)
                    : new IVMPlanAnalysis(IVMPlanPattern.SCAN_ONLY, null);
        }

        // --- Patterns to be implemented ---
        if (current instanceof LogicalAggregate) {
            return analyzeAggregate((LogicalAggregate<?>) current);
        }
        if (current instanceof LogicalJoin) {
            return analyzeJoin((LogicalJoin<?, ?>) current);
        }
        if (current instanceof LogicalUnion) {
            return analyzeUnion((LogicalUnion) current);
        }

        // --- Explicitly unsupported nodes ---
        if (current instanceof LogicalSort) {
            return unsupported("ORDER BY is not supported for IVM");
        }
        if (current instanceof LogicalLimit) {
            return unsupported("LIMIT is not supported for IVM");
        }
        if (current instanceof LogicalWindow) {
            return unsupported("Window functions are not supported for IVM");
        }
        if (current instanceof LogicalRepeat) {
            return unsupported("ROLLUP/CUBE/GROUPING SETS are not supported for IVM");
        }

        return unsupported("Unsupported plan node: " + current.getClass().getSimpleName());
    }

    private IVMPlanAnalysis analyzeFilter(LogicalFilter<?> filter) {
        Plan child = filter.child(0);

        // strip optional inner project below filter
        if (child instanceof LogicalProject) {
            LogicalProject<?> project = (LogicalProject<?>) child;
            if (project.isDistinct()) {
                return unsupported("SELECT DISTINCT below filter is not supported for IVM");
            }
            child = project.child(0);
        }

        if (child instanceof LogicalCatalogRelation) {
            return new IVMPlanAnalysis(IVMPlanPattern.FILTER_PROJECT_SCAN, null);
        }

        if (child instanceof LogicalJoin) {
            return analyzeJoin((LogicalJoin<?, ?>) child);
        }

        return unsupported("Unsupported plan below filter: " + child.getClass().getSimpleName());
    }

    /**
     * Analyzes a root-level UNION ALL. Only UNION ALL is supported;
     * UNION DISTINCT is rejected. Union must be at the plan root
     * (after stripping outer project).
     */
    private IVMPlanAnalysis analyzeUnion(LogicalUnion union) {
        if (union.getQualifier() != Qualifier.ALL) {
            return unsupported("Only UNION ALL is supported for IVM, got UNION DISTINCT");
        }
        return new IVMPlanAnalysis(IVMPlanPattern.UNION_ALL_ROOT, null);
    }

    private IVMPlanAnalysis analyzeAggregate(LogicalAggregate<?> aggregate) {
        // reject ROLLUP / CUBE / GROUPING SETS
        if (aggregate.getSourceRepeat().isPresent()) {
            return unsupported("ROLLUP/CUBE/GROUPING SETS are not supported for IVM");
        }

        // validate aggregate functions
        String aggError = validateAggFunctions(aggregate);
        if (aggError != null) {
            return unsupported(aggError);
        }

        // find the core node below aggregate, stripping optional project/filter
        Plan child = aggregate.child(0);
        if (child instanceof LogicalProject) {
            LogicalProject<?> project = (LogicalProject<?>) child;
            if (project.isDistinct()) {
                return unsupported("SELECT DISTINCT below aggregate is not supported for IVM");
            }
            child = project.child(0);
        }
        if (child instanceof LogicalFilter) {
            Plan filterChild = ((LogicalFilter<?>) child).child(0);
            if (filterChild instanceof LogicalProject) {
                LogicalProject<?> project = (LogicalProject<?>) filterChild;
                if (project.isDistinct()) {
                    return unsupported("SELECT DISTINCT below aggregate filter is not supported for IVM");
                }
                filterChild = project.child(0);
            }
            child = filterChild;
        }

        if (child instanceof LogicalCatalogRelation) {
            return new IVMPlanAnalysis(IVMPlanPattern.AGG_ON_SCAN, null);
        }
        if (child instanceof LogicalJoin) {
            String joinError = validateJoin((LogicalJoin<?, ?>) child);
            if (joinError != null) {
                return unsupported(joinError);
            }
            return new IVMPlanAnalysis(IVMPlanPattern.AGG_ON_INNER_JOIN, null);
        }

        return unsupported("Unsupported plan below aggregate: " + child.getClass().getSimpleName());
    }

    /**
     * Validates that all aggregate functions in the aggregate node are supported for IVM.
     * Currently supports: Sum, Count, Avg (Avg is rewritten to Sum+Count at create time).
     * DISTINCT aggregates are rejected.
     *
     * @return null if all valid, error message if not
     */
    private String validateAggFunctions(LogicalAggregate<?> aggregate) {
        for (AggregateFunction func : aggregate.getAggregateFunctions()) {
            if (func.isDistinct()) {
                return "DISTINCT aggregate is not supported for IVM: " + func.getName();
            }
            if (!(func instanceof Sum) && !(func instanceof Count) && !(func instanceof Avg)) {
                return "Unsupported aggregate function for IVM: " + func.getName()
                        + ". Only SUM, COUNT, AVG are supported";
            }
        }
        return null;
    }

    private IVMPlanAnalysis analyzeJoin(LogicalJoin<?, ?> join) {
        String error = validateJoin(join);
        if (error != null) {
            return unsupported(error);
        }
        return new IVMPlanAnalysis(IVMPlanPattern.INNER_JOIN, null);
    }

    /**
     * Recursively validates that a join tree only contains inner/cross joins
     * with supported child nodes.
     *
     * @return null if valid, error message if not
     */
    private String validateJoin(LogicalJoin<?, ?> join) {
        if (!join.getJoinType().isInnerOrCrossJoin()) {
            return "Only INNER JOIN and CROSS JOIN are supported for IVM, got: " + join.getJoinType();
        }
        if (join.getMarkJoinSlotReference().isPresent()) {
            return "Mark join (from subquery) is not supported for IVM";
        }
        String leftError = validateJoinChild(join.left());
        if (leftError != null) {
            return leftError;
        }
        return validateJoinChild(join.right());
    }

    /**
     * Validates that a join child subtree contains only supported nodes:
     * scans, filters, projects, or nested inner/cross joins.
     *
     * @return null if valid, error message if not
     */
    private String validateJoinChild(Plan child) {
        if (child instanceof LogicalCatalogRelation) {
            return null;
        }
        if (child instanceof LogicalProject) {
            LogicalProject<?> project = (LogicalProject<?>) child;
            if (project.isDistinct()) {
                return "SELECT DISTINCT in join child is not supported for IVM";
            }
            return validateJoinChild(project.child(0));
        }
        if (child instanceof LogicalFilter) {
            return validateJoinChild(((LogicalFilter<?>) child).child(0));
        }
        if (child instanceof LogicalJoin) {
            return validateJoin((LogicalJoin<?, ?>) child);
        }
        return "Unsupported node in join subtree: " + child.getClass().getSimpleName();
    }

    private IVMPlanAnalysis unsupported(String reason) {
        return new IVMPlanAnalysis(IVMPlanPattern.UNSUPPORTED, reason);
    }
}
