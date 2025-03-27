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
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.ComparableLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.ImmutableEqualSet;
import org.apache.doris.nereids.util.ImmutableEqualSet.Builder;
import org.apache.doris.nereids.util.PredicateInferUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * constant propagation, like: a = 10 and a + b > 30 => a = 10 and 10 + b > 30
 */
public class ConstantPropagation extends DefaultPlanRewriter<Void> implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        return plan.accept(this, null);
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, Void context) {
        filter = visitChildren(this, filter, context);
        Map<Slot, Literal> uniformConstants = filter.getLogicalProperties().getTrait().getAllUniformAndConstant();
        Map<Slot, Literal> childUniformConstants = filter.child().getLogicalProperties().getTrait()
                .getAllUniformAndConstant();
        Map<Slot, Boolean> myInferConstantSlots = uniformConstants.keySet().stream()
                .collect(Collectors.toMap(Function.identity(), e -> Boolean.FALSE));
        myInferConstantSlots.replaceAll((slot, included) -> childUniformConstants.containsKey(slot));
        Set<Expression> replacedConjuncts = Sets.newLinkedHashSetWithExpectedSize(filter.getConjuncts().size());
        ImmutableEqualSet<Slot> equalSet = filter.getLogicalProperties().getTrait().getEqualSlots();
        for (Expression expression : filter.getConjuncts()) {
            Expression newExpr = replaceWithConstant(expression, equalSet, uniformConstants, myInferConstantSlots);
            if (newExpr instanceof And) {
                replacedConjuncts.addAll(ExpressionUtils.extractConjunction(newExpr));
            } else {
                replacedConjuncts.add(newExpr);
            }
        }
        for (Map.Entry<Slot, Boolean> entry : myInferConstantSlots.entrySet()) {
            if (!entry.getValue()) {
                Slot slot = entry.getKey();
                replacedConjuncts.add(new EqualTo(slot, uniformConstants.get(slot)));
            }
        }

        return filter.withConjunctsAndChild(replacedConjuncts, filter.child());
    }

    private Expression replaceWithConstant(Expression expression, ImmutableEqualSet<Slot> equalSet,
            Map<Slot, Literal> constants, Map<Slot, Boolean> myInferConstantSlots) {
        if (!needReplaceWithConstant(expression, constants, myInferConstantSlots)) {
            return expression;
        }

        return replaceWithConstantRecursive(expression, equalSet, constants);
    }

    private Expression replaceWithConstantRecursive(Expression expression,
            ImmutableEqualSet<Slot> parentEqualSet, Map<Slot, Literal> parentConstants) {
        if (expression instanceof And) {
            return replaceAndWithConstant((And) expression, parentEqualSet, parentConstants);
        } else if (expression instanceof Or) {
            return replaceOrWithConstant((Or) expression, parentEqualSet, parentConstants);
        } else {
            return ExpressionUtils.replace(expression, parentConstants);
        }
    }

    private Expression replaceAndWithConstant(And expression,
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
                newChild = ExpressionUtils.replace(child, newConstants);
                newChild = replaceWithConstantRecursive(newChild, newEqualSet, newConstants);
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

    private Expression replaceOrWithConstant(Or expression,
            ImmutableEqualSet<Slot> parentEqualSet, Map<Slot, Literal> parentConstants) {
        List<Expression> disjunctions = ExpressionUtils.extractDisjunction(expression);
        ImmutableList.Builder<Expression> builder = ImmutableList.builderWithExpectedSize(disjunctions.size());
        for (Expression child : disjunctions) {
            Expression newChild = ExpressionUtils.replace(child, parentConstants);
            newChild = replaceWithConstantRecursive(newChild, parentEqualSet, parentConstants);
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
}
