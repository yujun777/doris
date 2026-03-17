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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MurmurHash364;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Uuid;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Rewrites a materialized view's plan at CREATE time for IVM compatibility.
 *
 * <p>This rewriter runs once during MV creation, not at refresh time. It:
 * <ol>
 *   <li>Adds a {@code __ivm_row_id__} column propagated bottom-up through
 *       the plan tree (the MV uses this as its unique/primary key)</li>
 *   <li>Rewrites {@code avg(x)} to {@code sum(x)} + {@code count(x)}</li>
 *   <li>Adds {@code count(*)} if the aggregate pattern doesn't already
 *       contain a count (needed for retraction bookkeeping)</li>
 * </ol>
 *
 * <p>RowId propagation rules:
 * <ul>
 *   <li>OlapScan (unique key MOW): {@code murmur_hash3_64(key_cols)}</li>
 *   <li>OlapScan (dup key): {@code uuid()} (non-deterministic)</li>
 *   <li>Filter/Project: transparent pass-through from child</li>
 *   <li>Join: {@code murmur_hash3_64(cast(left.__ivm_row_id__ as varchar),
 *       cast(right.__ivm_row_id__ as varchar))}</li>
 *   <li>Aggregate: {@code murmur_hash3_64(cast(group_key_1 as varchar),
 *       cast(group_key_2 as varchar), ...)}</li>
 * </ul>
 */
public class IVMCreateSqlRewriter {

    /** Column name for the IVM row identifier added to the MV. */
    public static final String IVM_ROW_ID_COLUMN = "__ivm_row_id__";

    /** Column name for the IVM count bookkeeping column. */
    public static final String IVM_COUNT_COLUMN = "__ivm_count__";

    /**
     * Rewrites the original MV plan for IVM support.
     *
     * @param originalPlan the original MV definition plan
     * @return the rewritten plan with rowId and bookkeeping columns added
     * @throws AnalysisException if the plan cannot be rewritten for IVM
     */
    public Plan rewriteForCreate(Plan originalPlan) throws AnalysisException {
        RewriteResult result = rewriteBottomUp(originalPlan);
        return result.plan;
    }

    /**
     * Recursively rewrites the plan bottom-up, propagating rowId expressions
     * and rewriting aggregate functions.
     */
    private RewriteResult rewriteBottomUp(Plan plan) throws AnalysisException {
        if (plan instanceof LogicalOlapScan) {
            return rewriteScan((LogicalOlapScan) plan);
        }
        if (plan instanceof LogicalCatalogRelation) {
            throw new AnalysisException("External base table does not support IVM rewrite: "
                    + plan.getClass().getSimpleName());
        }
        if (plan instanceof LogicalFilter) {
            return rewriteFilter((LogicalFilter<?>) plan);
        }
        if (plan instanceof LogicalProject) {
            return rewriteProject((LogicalProject<?>) plan);
        }
        if (plan instanceof LogicalJoin) {
            return rewriteJoin((LogicalJoin<?, ?>) plan);
        }
        if (plan instanceof LogicalAggregate) {
            return rewriteAggregate((LogicalAggregate<?>) plan);
        }

        throw new AnalysisException(
                "Unsupported plan node for IVM rewrite: "
                + plan.getClass().getSimpleName());
    }

    /**
     * Rewrites an OlapScan: adds a project on top with the rowId column.
     *
     * <p>For merge-on-write unique key tables, rowId = murmur_hash3_64(key columns).
     * For duplicate key tables, rowId = uuid() (non-deterministic).
     * Other key types are rejected because they do not provide binlog/stream support for IVM.
     */
    private RewriteResult rewriteScan(LogicalOlapScan scan) throws AnalysisException {
        OlapTable olapTable = scan.getTable();
        KeysType keysType = olapTable.getKeysType();

        Expression rowIdExpr;
        boolean isDeterministic;

        if (keysType == KeysType.UNIQUE_KEYS) {
            if (!olapTable.getEnableUniqueKeyMergeOnWrite()) {
                throw new AnalysisException("Merge-on-read unique key table does not support binlog and stream"
                        + " for IVM rewrite: " + olapTable.getName());
            }
            // hash(key columns)
            List<Column> keyColumns = olapTable.getBaseSchemaKeyColumns();
            List<Slot> scanOutput = scan.getOutput();
            List<Expression> keySlots = new ArrayList<>();
            for (Column keyCol : keyColumns) {
                Slot slot = findSlotByColumnName(scanOutput, keyCol.getName());
                if (slot != null) {
                    keySlots.add(new Cast(slot, VarcharType.SYSTEM_DEFAULT));
                }
            }
            rowIdExpr = buildHashExpr(keySlots);
            isDeterministic = true;
        } else if (keysType == KeysType.DUP_KEYS) {
            // detail table: non-deterministic uuid-based rowId
            rowIdExpr = new Uuid();
            isDeterministic = false;
        } else {
            throw new AnalysisException("Table key type does not support binlog and stream for IVM rewrite: "
                    + keysType + ", table=" + olapTable.getName());
        }

        Alias rowIdAlias = new Alias(rowIdExpr, IVM_ROW_ID_COLUMN);
        List<NamedExpression> projects = new ArrayList<>(scan.getOutput());
        projects.add(rowIdAlias);
        LogicalProject<Plan> project = new LogicalProject<>(projects, scan);
        return new RewriteResult(project, rowIdAlias.toSlot(), isDeterministic);
    }

    /**
     * Rewrites a Filter: transparent pass-through. Rebuilds child, then
     * wraps with the same filter predicate.
     */
    private RewriteResult rewriteFilter(LogicalFilter<?> filter) throws AnalysisException {
        RewriteResult childResult = rewriteBottomUp(filter.child(0));
        Plan newFilter = filter.withChildren(ImmutableList.of(childResult.plan));
        return new RewriteResult(newFilter, childResult.rowIdSlot, childResult.isDeterministic);
    }

    /**
     * Rewrites a Project: transparent pass-through. Adds the rowId slot
     * to the projection list so it's carried forward.
     */
    private RewriteResult rewriteProject(LogicalProject<?> project) throws AnalysisException {
        RewriteResult childResult = rewriteBottomUp(project.child(0));

        // Add rowId to the projection list
        List<NamedExpression> newProjects = new ArrayList<>(project.getProjects());
        newProjects.add(childResult.rowIdSlot);
        Plan newProject = project.withProjectsAndChild(newProjects,
                childResult.plan);
        return new RewriteResult(newProject, childResult.rowIdSlot, childResult.isDeterministic);
    }

    /**
     * Rewrites a Join: rowId = murmur_hash3_64(left.rowId, right.rowId).
     */
    private RewriteResult rewriteJoin(LogicalJoin<?, ?> join) throws AnalysisException {
        RewriteResult leftResult = rewriteBottomUp(join.left());
        RewriteResult rightResult = rewriteBottomUp(join.right());

        Plan newJoin = join.withChildren(
                ImmutableList.of(leftResult.plan, rightResult.plan));

        // Combine rowIds: hash(left.rowId, right.rowId)
        Expression combinedRowId = buildHashExpr(ImmutableList.of(
                new Cast(leftResult.rowIdSlot, VarcharType.SYSTEM_DEFAULT),
                new Cast(rightResult.rowIdSlot, VarcharType.SYSTEM_DEFAULT)));
        Alias rowIdAlias = new Alias(combinedRowId, IVM_ROW_ID_COLUMN);

        // Add a project on top of the join to carry the combined rowId
        List<NamedExpression> projects = new ArrayList<>();
        for (Slot slot : newJoin.getOutput()) {
            projects.add(slot);
        }
        projects.add(rowIdAlias);
        LogicalProject<Plan> projectOnJoin = new LogicalProject<>(projects, newJoin);

        boolean isDeterministic = leftResult.isDeterministic && rightResult.isDeterministic;
        return new RewriteResult(projectOnJoin, rowIdAlias.toSlot(), isDeterministic);
    }

    /**
     * Rewrites an Aggregate:
     * <ol>
     *   <li>rowId = murmur_hash3_64(group by keys)</li>
     *   <li>avg(x) -> sum(x) + count(x)</li>
     *   <li>Add count(*) as __ivm_count__ if no count exists</li>
     * </ol>
     */
    private RewriteResult rewriteAggregate(
            LogicalAggregate<?> aggregate) throws AnalysisException {
        RewriteResult childResult = rewriteBottomUp(aggregate.child(0));

        // Build rowId from group-by keys
        List<Expression> groupByExprs = aggregate.getGroupByExpressions();
        Expression rowIdExpr;
        if (groupByExprs.isEmpty()) {
            // Scalar aggregate (no group by) — single row, constant rowId
            rowIdExpr = new Cast(
                    new org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral(0L),
                    BigIntType.INSTANCE);
        } else {
            List<Expression> castKeys = groupByExprs.stream()
                    .map(key -> (Expression) new Cast(key, VarcharType.SYSTEM_DEFAULT))
                    .collect(Collectors.toList());
            rowIdExpr = buildHashExpr(castKeys);
        }
        Alias rowIdAlias = new Alias(rowIdExpr, IVM_ROW_ID_COLUMN);

        // Rewrite output expressions: handle avg -> sum+count
        List<NamedExpression> newOutputExprs = new ArrayList<>();
        boolean hasCount = false;

        for (NamedExpression outputExpr : aggregate.getOutputExpressions()) {
            List<NamedExpression> rewritten = rewriteAggOutputExpr(outputExpr);
            newOutputExprs.addAll(rewritten);

            // Check if any output contains a count
            for (NamedExpression expr : rewritten) {
                if (containsCount(expr)) {
                    hasCount = true;
                }
            }
        }

        // Add __ivm_count__ if no count exists
        if (!hasCount) {
            Alias countAlias = new Alias(new Count(), IVM_COUNT_COLUMN);
            newOutputExprs.add(countAlias);
        }

        // Add rowId to output
        newOutputExprs.add(rowIdAlias);

        Plan newAggregate = aggregate.withChildGroupByAndOutput(
                aggregate.getGroupByExpressions(),
                newOutputExprs,
                childResult.plan);

        return new RewriteResult(newAggregate, rowIdAlias.toSlot(), true);
    }

    /**
     * Rewrites a single aggregate output expression.
     * If it contains avg(x), replaces it with sum(x) and count(x).
     * Otherwise returns the expression unchanged.
     */
    private List<NamedExpression> rewriteAggOutputExpr(NamedExpression expr) {
        List<NamedExpression> result = new ArrayList<>();

        if (expr instanceof Alias) {
            Alias alias = (Alias) expr;
            Expression child = alias.child();

            if (child instanceof Avg) {
                Avg avg = (Avg) child;
                Expression avgArg = avg.child(0);
                // avg(x) -> sum(x) as <name>__ivm_sum__, count(x) as <name>__ivm_count__
                result.add(new Alias(new Sum(avgArg), alias.getName() + "__ivm_sum__"));
                result.add(new Alias(new Count(avgArg), alias.getName() + "__ivm_count__"));
                return result;
            }
        }

        result.add(expr);
        return result;
    }

    /**
     * Checks whether an expression tree contains a Count aggregate function.
     */
    private boolean containsCount(Expression expr) {
        if (expr instanceof Count) {
            return true;
        }
        if (expr instanceof Alias) {
            return containsCount(((Alias) expr).child());
        }
        for (Expression child : expr.children()) {
            if (containsCount(child)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Builds a murmur_hash3_64 expression from the given arguments.
     * Arguments should already be cast to varchar.
     */
    private Expression buildHashExpr(List<Expression> args) {
        if (args.isEmpty()) {
            return new Cast(
                    new org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral(0L),
                    BigIntType.INSTANCE);
        }
        Expression first = args.get(0);
        Expression[] rest = args.subList(1, args.size()).toArray(new Expression[0]);
        return new MurmurHash364(first, rest);
    }

    /**
     * Finds a slot in the output list matching the given column name.
     */
    private Slot findSlotByColumnName(List<Slot> output, String columnName) {
        for (Slot slot : output) {
            if (slot.getName().equalsIgnoreCase(columnName)) {
                return slot;
            }
        }
        return null;
    }

    /**
     * Intermediate result of rewriting a plan subtree.
     * Carries the rewritten plan, the rowId slot for the parent to reference,
     * and whether the rowId is deterministic.
     */
    private static class RewriteResult {
        final Plan plan;
        final Slot rowIdSlot;
        final boolean isDeterministic;

        RewriteResult(Plan plan, Slot rowIdSlot, boolean isDeterministic) {
            this.plan = plan;
            this.rowIdSlot = rowIdSlot;
            this.isDeterministic = isDeterministic;
        }
    }
}
