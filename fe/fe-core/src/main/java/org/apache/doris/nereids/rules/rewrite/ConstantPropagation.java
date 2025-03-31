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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.expression.ExpressionNormalizationAndOptimization;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.ComparableLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.ImmutableEqualSet;
import org.apache.doris.nereids.util.ImmutableEqualSet.Builder;
import org.apache.doris.nereids.util.PredicateInferUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * constant propagation, like: a = 10 and a + b > 30 => a = 10 and 10 + b > 30
 */
public class ConstantPropagation extends DefaultPlanRewriter<ExpressionRewriteContext> implements CustomRewriter {

    private final ExpressionNormalizationAndOptimization exprNormalAndOpt
            = new ExpressionNormalizationAndOptimization(false);

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        ExpressionRewriteContext context = new ExpressionRewriteContext(jobContext.getCascadesContext());
        return plan.accept(this, context);
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, ExpressionRewriteContext context) {
        filter = visitChildren(this, filter, context);
        Expression oldPredicate = filter.getPredicate();
        Expression newPredicate = replaceConstantsAndRewriteExpr(filter, oldPredicate, context);
        if (isExprEqualIgnoreOrder(oldPredicate, newPredicate)) {
            return filter;
        } else {
            Set<Expression> newConjuncts = Sets.newLinkedHashSet(ExpressionUtils.extractConjunction(newPredicate));
            return filter.withConjunctsAndChild(newConjuncts, filter.child());
        }
    }

    @Override
    public Plan visitLogicalHaving(LogicalHaving<? extends Plan> having, ExpressionRewriteContext context) {
        having = visitChildren(this, having, context);
        Expression oldPredicate = having.getPredicate();
        Expression newPredicate = replaceConstantsAndRewriteExpr(having, oldPredicate, context);
        if (isExprEqualIgnoreOrder(oldPredicate, newPredicate)) {
            return having;
        } else {
            Set<Expression> newConjuncts = Sets.newLinkedHashSet(ExpressionUtils.extractConjunction(newPredicate));
            return having.withConjunctsAndChild(newConjuncts, having.child());
        }
    }

    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> project, ExpressionRewriteContext context) {
        project = visitChildren(this, project, context);
        Pair<ImmutableEqualSet<Slot>, Map<Slot, Literal>> childEqualTrait = getChildEqualSetAndConstants(project);
        List<NamedExpression> newProjects = project.getProjects().stream()
                .map(expr -> replaceNameExpressionConstants(expr, childEqualTrait.first, childEqualTrait.second))
                .collect(ImmutableList.toImmutableList());
        return newProjects.equals(project.getProjects()) ? project : project.withProjects(newProjects);
    }

    @Override
    public Plan visitLogicalSort(LogicalSort<? extends Plan> sort, ExpressionRewriteContext context) {
        sort = visitChildren(this, sort, context);
        Pair<ImmutableEqualSet<Slot>, Map<Slot, Literal>> childEqualTrait = getChildEqualSetAndConstants(sort);
        List<OrderKey> newOrderKeys = sort.getOrderKeys().stream()
                .map(key -> key.withExpression(replaceConstants(
                        key.getExpr(), childEqualTrait.first, childEqualTrait.second)))
                .collect(ImmutableList.toImmutableList());
        return newOrderKeys.equals(sort.getOrderKeys()) ? sort : sort.withOrderKeys(newOrderKeys);
    }

    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, ExpressionRewriteContext context) {
        join = visitChildren(this, join, context);
        List<Expression> allJoinConjuncts = Stream.concat(join.getHashJoinConjuncts().stream(),
                        join.getOtherJoinConjuncts().stream())
                .collect(ImmutableList.toImmutableList());
        Expression oldPredicate = ExpressionUtils.and(allJoinConjuncts);
        Expression newPredicate = replaceConstantsAndRewriteExpr(join, oldPredicate, context);
        if (isExprEqualIgnoreOrder(oldPredicate, newPredicate)) {
            return join;
        }
        ImmutableList.Builder<Expression> hashJoinConjunctsBuilder = ImmutableList.builderWithExpectedSize(
                join.getHashJoinConjuncts().size());
        ImmutableList.Builder<Expression> otherJoinConjunctsBuilder = ImmutableList.builderWithExpectedSize(
                join.getOtherJoinConjuncts().size());
        Set<Slot> leftOutputs = join.left().getOutputSet();
        Set<Slot> rightOutputs = join.right().getOutputSet();
        for (Expression conjunct : ExpressionUtils.extractConjunction(newPredicate)) {
            boolean isHashJoinConjunct = false;
            if (conjunct instanceof EqualPredicate) {
                Set<Slot> leftSlots = conjunct.child(0).collect(SlotReference.class::isInstance);
                Set<Slot> rightSlots = conjunct.child(1).collect(SlotReference.class::isInstance);
                if (!leftSlots.isEmpty() && !rightSlots.isEmpty()
                        && (leftOutputs.containsAll(leftSlots) && rightOutputs.containsAll(rightSlots)
                                || leftOutputs.containsAll(rightSlots) && rightOutputs.containsAll(leftSlots))) {
                    isHashJoinConjunct = true;
                }
            }
            if (isHashJoinConjunct) {
                hashJoinConjunctsBuilder.add(conjunct);
            } else {
                otherJoinConjunctsBuilder.add(conjunct);
            }
        }

        List<Expression> newHashJoinConjuncts = hashJoinConjunctsBuilder.build();
        List<Expression> newOtherJoinConjuncts = otherJoinConjunctsBuilder.build();

        return newHashJoinConjuncts.equals(join.getHashJoinConjuncts())
                && newOtherJoinConjuncts.equals(join.getOtherJoinConjuncts())
                ? join
                : join.withJoinConjuncts(newHashJoinConjuncts, newOtherJoinConjuncts, join.getJoinReorderContext());
    }

    /**
     * replace constants
     */
    @VisibleForTesting
    public Expression replaceConstantsAndRewriteExpr(LogicalPlan plan, Expression expression,
            ExpressionRewriteContext context) {
        Pair<ImmutableEqualSet<Slot>, Map<Slot, Literal>> childEqualTrait = getChildEqualSetAndConstants(plan);
        Expression afterExpression = expression;
        for (int i = 0; i < 100; i++) {
            Expression beforeExpression = afterExpression;
            afterExpression = replaceConstants(beforeExpression, childEqualTrait.first, childEqualTrait.second);
            if (isExprEqualIgnoreOrder(beforeExpression, afterExpression)) {
                break;
            }
            beforeExpression = afterExpression;
            afterExpression = exprNormalAndOpt.rewrite(beforeExpression, context);
            if (isExprEqualIgnoreOrder(beforeExpression, afterExpression)) {
                break;
            }
        }
        return afterExpression;
    }

    private NamedExpression replaceNameExpressionConstants(NamedExpression expr, ImmutableEqualSet<Slot> equalSet,
            Map<Slot, Literal> constants) {
        Expression newExpr = replaceConstants(expr, equalSet, constants);
        if (newExpr instanceof NamedExpression) {
            return (NamedExpression) newExpr;
        } else {
            return new Alias(expr.getExprId(), newExpr, expr.getName());
        }
    }

    private Expression replaceConstants(Expression expression, ImmutableEqualSet<Slot> parentEqualSet,
            Map<Slot, Literal> parentConstants) {
        if (expression instanceof And) {
            return replaceAndConstants((And) expression, parentEqualSet, parentConstants);
        } else if (expression instanceof Or) {
            return replaceOrConstants((Or) expression, parentEqualSet, parentConstants);
        } else if (!parentConstants.isEmpty()
                && expression.anyMatch(e -> e instanceof Slot && parentConstants.containsKey(e))) {
            return ExpressionUtils.replace(expression, parentConstants);
        } else {
            return expression;
        }
    }

    private Expression replaceAndConstants(And expression,
            ImmutableEqualSet<Slot> parentEqualSet, Map<Slot, Literal> parentConstants) {
        List<Expression> conjunctions = ExpressionUtils.extractConjunction(expression);
        Optional<Pair<ImmutableEqualSet<Slot>, Map<Slot, Literal>>> equalAndConstantOptions =
                expandEqualSetAndConstants(conjunctions, parentEqualSet, parentConstants);
        // infer conflict constants like a = 10 and a = 30
        if (!equalAndConstantOptions.isPresent()) {
            return BooleanLiteral.FALSE;
        }
        ImmutableEqualSet<Slot> newEqualSet = equalAndConstantOptions.get().first;
        Map<Slot, Literal> newConstants = equalAndConstantOptions.get().second;
        Map<Slot, Boolean> myInferConstantSlots = newConstants.keySet().stream()
                .filter(slot -> !parentConstants.containsKey(slot))
                .collect(Collectors.toMap(Function.identity(), e -> Boolean.FALSE));
        ImmutableList.Builder<Expression> builder = ImmutableList.builderWithExpectedSize(conjunctions.size());
        for (Expression child : conjunctions) {
            Expression newChild = child;
            if (needReplaceWithConstant(newChild, newConstants, myInferConstantSlots)) {
                newChild = replaceConstants(newChild, newEqualSet, newConstants);
            }
            if (newChild.equals(BooleanLiteral.FALSE)) {
                return BooleanLiteral.FALSE;
            }
            if (newChild instanceof And) {
                builder.addAll(ExpressionUtils.extractConjunction(newChild));
            } else {
                builder.add(newChild);
            }
        }
        for (Map.Entry<Slot, Boolean> entry : myInferConstantSlots.entrySet()) {
            if (!entry.getValue()) {
                Slot slot = entry.getKey();
                builder.add(new EqualTo(slot, newConstants.get(slot)));
            }
        }
        return expression.withChildren(builder.build());
    }

    private Expression replaceOrConstants(Or expression,
            ImmutableEqualSet<Slot> parentEqualSet, Map<Slot, Literal> parentConstants) {
        List<Expression> disjunctions = ExpressionUtils.extractDisjunction(expression);
        ImmutableList.Builder<Expression> builder = ImmutableList.builderWithExpectedSize(disjunctions.size());
        for (Expression child : disjunctions) {
            Expression newChild = replaceConstants(child, parentEqualSet, parentConstants);
            if (newChild.equals(BooleanLiteral.TRUE)) {
                return BooleanLiteral.TRUE;
            }
            builder.add(newChild);
        }
        return expression.withChildren(builder.build());
    }

    private boolean needReplaceWithConstant(Expression expression, Map<Slot, Literal> constants,
            Map<Slot, Boolean> myInferConstantSlots) {
        if (expression instanceof EqualTo && expression.child(0) instanceof Slot) {
            Slot slot = (Slot) expression.child(0);
            if (myInferConstantSlots.get(slot) == Boolean.FALSE
                    && expression.child(1).equals(constants.get(slot))) {
                myInferConstantSlots.put(slot, true);
                return false;
            }
        }

        return true;
    }

    private Pair<ImmutableEqualSet<Slot>, Map<Slot, Literal>> getChildEqualSetAndConstants(
            LogicalPlan plan) {
        if (plan.children().size() == 1) {
            DataTrait dataTrait = plan.child(0).getLogicalProperties().getTrait();
            return Pair.of(dataTrait.getEqualSet(), dataTrait.getAllUniformAndConstant());
        } else {
            Map<Slot, Literal> uniformConstants = Maps.newHashMap();
            ImmutableEqualSet.Builder<Slot> newEqualSetBuilder = new Builder<>();
            for (Plan child : plan.children()) {
                uniformConstants.putAll(child.getLogicalProperties().getTrait().getAllUniformAndConstant());
                newEqualSetBuilder.addEqualSet(child.getLogicalProperties().getTrait().getEqualSet());
            }
            return Pair.of(newEqualSetBuilder.build(), uniformConstants);
        }
    }

    // if had conflict constants relation, return optional.empty()
    private Optional<Pair<ImmutableEqualSet<Slot>, Map<Slot, Literal>>> expandEqualSetAndConstants(
            List<Expression> conjunctions,
            ImmutableEqualSet<Slot> parentEqualSet,
            Map<Slot, Literal> parentConstants) {
        Map<Slot, Literal> newConstants = Maps.newHashMapWithExpectedSize(parentConstants.size());
        newConstants.putAll(parentConstants);
        ImmutableEqualSet.Builder<Slot> newEqualSetBuilder = new Builder<>(parentEqualSet);
        for (Expression child : conjunctions) {
            Optional<Pair<Slot, Expression>> equalItem = findValidEqualItem(child);
            if (!equalItem.isPresent()) {
                continue;
            }
            Slot slot = equalItem.get().first;
            Expression expr = equalItem.get().second;
            if (expr instanceof Slot) {
                newEqualSetBuilder.addEqualPair(slot, (Slot) expr);
            } else if (!addConstant(newConstants, slot, (Literal) expr)) {
                return Optional.empty();
            }
        }

        ImmutableEqualSet<Slot> newEqualSet = newEqualSetBuilder.build();
        List<Set<Slot>> multiEqualSlots = newEqualSet.calEqualSetList();
        for (Set<Slot> slots : multiEqualSlots) {
            Slot slot = slots.stream().filter(newConstants::containsKey).findFirst().orElse(null);
            if (slot == null) {
                continue;
            }
            Literal value = newConstants.get(slot);
            for (Slot s : slots) {
                if (!addConstant(newConstants, s, value)) {
                    return Optional.empty();
                }
            }
        }

        return Optional.of(Pair.of(newEqualSet, newConstants));
    }

    private boolean addConstant(Map<Slot, Literal> constants, Slot slot, Literal value) {
        Literal existValue = constants.get(slot);
        if (existValue == null) {
            constants.put(slot, value);
            return true;
        }
        return value.equals(existValue)
                || value instanceof ComparableLiteral && existValue instanceof ComparableLiteral
                        && ((ComparableLiteral) value).compareTo((ComparableLiteral) existValue) == 0;
    }

    private Optional<Pair<Slot, Expression>> findValidEqualItem(Expression expression) {
        if (!(expression instanceof EqualPredicate)) {
            return Optional.empty();
        }

        Expression left = expression.child(0);
        Expression right = expression.child(1);
        if (!PredicateInferUtils.isSlotOrNotNullLiteral(left) || !PredicateInferUtils.isSlotOrNotNullLiteral(right)) {
            return Optional.empty();
        }

        if (left instanceof Slot) {
            return Optional.of(Pair.of((Slot) left, right));
        } else if (right instanceof Slot) {
            return Optional.of(Pair.of((Slot) right, left));
        } else {
            return Optional.empty();
        }
    }

    private boolean isExprEqualIgnoreOrder(Expression oldExpr, Expression newExpr) {
        if (oldExpr instanceof And) {
            return Sets.newHashSet(ExpressionUtils.extractConjunction(oldExpr))
                    .equals(Sets.newHashSet(ExpressionUtils.extractConjunction(newExpr)));
        } else if (oldExpr instanceof Or) {
            return Sets.newHashSet(ExpressionUtils.extractDisjunction(oldExpr))
                    .equals(Sets.newHashSet(ExpressionUtils.extractDisjunction(newExpr)));
        } else {
            return oldExpr.equals(newExpr);
        }
    }
}
