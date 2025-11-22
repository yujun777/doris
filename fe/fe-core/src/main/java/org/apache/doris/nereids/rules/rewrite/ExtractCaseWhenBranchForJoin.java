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
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NullIf;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

// Extract case when branches to OR expressions for join conditions.
// Latter can help to generate more join conditions.
//
// 1. extract conditions for one side only, latter can push the filter down:
//
//    t1 join t2 on not (case when t1.a = 1 then t2.a else t2.b) + t2.b + t2.c > 10)
//    =>
//    t1 join t2 on not (case when t1.a = 1 then t2.a else t2.b end) + t2.b + t2.c > 10)
//                  AND (not (t2.a + t2.b + t2.c > 10) or not (t2.b + t2.b + t2.c > 10))
//
// 2. extract hash conditions for both sides, latter can convert cross join to hash join:
//    the hash condition need to be equal predicate, and one side contains only left side slots,
//    another side contains only right side slots.
//
//    t1 join t2 on (case when t1.a = 1 then t2.a else t2.b end) = t1.a + t1.b
//    =>
//    t1 join t2 on (case when t1.a = 1 then t2.a else t2.b end) = t1.a + t1.b
//                AND (t2.a = t1.a + t1.b or t2.b = t1.a + t1.b)

// Notice we don't extract more than one case when like expressions.
// because it may generate expressions with combinatorial explosion.
//
// (((case c1 then p1 else p2 end) + (case when d1 then q1 else q2 end))) + a  > 10
// => (p1 + q1 + a > 10)
//     or (p1 + q2 + a > 10)
//     or (p2 + q1 + a > 10)
//     or (p2 + q2 + a > 10)
//
// so we only extract at most one case when like expression for each condition.

public class ExtractCaseWhenBranchForJoin implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(logicalJoin()
                .when(this::needRewrite)
                .then(this::rewrite)
                .toRule(RuleType.EXTRACT_CASE_WHEN_BRANCH_FOR_JOIN));
    }

    private boolean needRewrite(LogicalJoin<?, ?> join) {
        Set<Slot> leftSlots = join.left().getOutputSet();
        Set<Slot> rightSlots = join.right().getOutputSet();
        for (Expression expr : join.getOtherJoinConjuncts()) {
            if (isConditionNeedRewrite(expr, leftSlots, rightSlots)) {
                return true;
            }
        }
        return false;
    }

    // 1. expr contains slots from both sides;
    private boolean isConditionNeedRewrite(Expression expr, Set<Slot> leftSlots, Set<Slot> rightSlots) {
        return getRewriteChildIndexAndAllChildFromLeft(expr, leftSlots, rightSlots).isPresent();
    }

    private Plan rewrite(LogicalJoin<?, ?> join) {
        List<Expression> extractHashConditions = Lists.newArrayList();
        List<Expression> extractOtherConditions = Lists.newArrayList();
        Set<Slot> leftSlots = join.left().getOutputSet();
        Set<Slot> rightSlots = join.right().getOutputSet();
        for (Expression expr : join.getOtherJoinConjuncts()) {
            extractOrExpression(expr, leftSlots, rightSlots, extractHashConditions, extractOtherConditions);
        }
        if (extractHashConditions.isEmpty() && extractOtherConditions.isEmpty()) {
            return join;
        }
        List<Expression> newHashConditions = join.getHashJoinConjuncts();
        List<Expression> newOtherConditions = join.getOtherJoinConjuncts();
        Set<Expression> dedupConditions = Sets.newHashSet();
        dedupConditions.addAll(join.getOtherJoinConjuncts());
        dedupConditions.addAll(join.getHashJoinConjuncts());
        if (!extractHashConditions.isEmpty()) {
            ImmutableList.Builder<Expression> hashConditionBuilder = ImmutableList.builderWithExpectedSize(
                    join.getHashJoinConjuncts().size() + extractHashConditions.size());
            hashConditionBuilder.addAll(join.getHashJoinConjuncts());
            for (Expression expr : extractHashConditions) {
                if (dedupConditions.add(expr)) {
                    hashConditionBuilder.add(expr);
                }
            }
            newHashConditions = hashConditionBuilder.build();
        }
        if (!extractOtherConditions.isEmpty()) {
            ImmutableList.Builder<Expression> otherConditionBuilder = ImmutableList.builderWithExpectedSize(
                    join.getOtherJoinConjuncts().size() + extractOtherConditions.size());
            otherConditionBuilder.addAll(join.getOtherJoinConjuncts());
            for (Expression expr : extractOtherConditions) {
                if (dedupConditions.add(expr)) {
                    otherConditionBuilder.add(expr);
                }
            }
            newOtherConditions = otherConditionBuilder.build();
        }
        if (newHashConditions.size() == join.getHashJoinConjuncts().size()
                && newOtherConditions.size() == join.getOtherJoinConjuncts().size()) {
            return join;
        }

        JoinType joinType = join.getJoinType();
        if (joinType == JoinType.CROSS_JOIN && !newHashConditions.isEmpty()) {
            joinType = JoinType.INNER_JOIN;
        }
        return new LogicalJoin<>(joinType,
                newHashConditions,
                newOtherConditions,
                join.getMarkJoinConjuncts(),
                join.getDistributeHint(),
                join.getMarkJoinSlotReference(),
                join.children(), join.getJoinReorderContext());
    }

    private void extractOrExpression(Expression expr, Set<Slot> leftSlots, Set<Slot> rightSlots,
            List<Expression> extractHashConditions, List<Expression> extractOtherConditions) {
        Optional<Pair<Integer, Boolean>> rewriteOpt = getRewriteChildIndexAndAllChildFromLeft(expr, leftSlots, rightSlots);
        if (!rewriteOpt.isPresent()) {
            return;
        }
        
        int rewriteChildIndex = rewriteOpt.get().first;
        Boolean otherChildrenFromLeft = rewriteOpt.get().second;
        if (otherChildrenFromLeft == null) {
            doExtractExpression(expr, rewriteChildIndex, true, leftSlots, rightSlots)
                    .ifPresent(extractOtherConditions::add);
            doExtractExpression(expr, rewriteChildIndex, false, leftSlots, rightSlots)
                    .ifPresent(extractOtherConditions::add);
        } else {
            doExtractExpression(expr, rewriteChildIndex, otherChildrenFromLeft, leftSlots, rightSlots)
                    .ifPresent(extractOtherConditions::add);
            if (expr instanceof EqualPredicate) {
                // one child contains one side slots, another child contains another side.
                doExtractExpression(expr, rewriteChildIndex, !otherChildrenFromLeft, leftSlots, rightSlots)
                        .filter(e -> !Collections.disjoint(expr.getInputSlots(), leftSlots)
                                        && !Collections.disjoint(expr.getInputSlots(), rightSlots))
                        .ifPresent(extractHashConditions::add);
            }
        }
    }
    
    private Optional<Pair<Integer, Boolean>> getRewriteChildIndexAndAllChildFromLeft(Expression expr,
            Set<Slot> leftSlots, Set<Slot> rightSlots) {
        if (expr.containsUniqueFunction()) {
            return Optional.empty();
        }
        int rewriteChildIndex = -1;
        Boolean otherChildrenFromLeft = null;
        for (int i = 0; i < expr.children().size(); i++) {
            Expression child = expr.child(i);
            Set<Slot> childSlots = child.getInputSlots();
            if (childSlots.isEmpty()) {
                continue;
            }
            boolean containsLeft = !Collections.disjoint(childSlots, leftSlots);
            boolean containsRight = !Collections.disjoint(childSlots, rightSlots);
            if (containsLeft && containsRight) {
                if (rewriteChildIndex != -1 || !ExpressionUtils.containsCaseWhenLikeType(child)) {
                    // more than one child contains both side slots
                    return Optional.empty();
                }
                rewriteChildIndex = i;
            } else if (containsLeft) {
                if (otherChildrenFromLeft == null) {
                    otherChildrenFromLeft = true;
                } else if (!otherChildrenFromLeft) {
                    // one child from left, another child from right
                    return Optional.empty();
                }
            } else if (containsRight) {
                if (otherChildrenFromLeft == null) {
                    otherChildrenFromLeft = false;
                } else if (otherChildrenFromLeft) {
                    // one child from left, another child from right
                    return Optional.empty();
                }
            } else {
                // should not be here
                return Optional.empty();
            }
        }

        if (rewriteChildIndex == -1) {
            return Optional.empty();
        }

        return Optional.of(Pair.of(rewriteChildIndex, otherChildrenFromLeft));
    }

    private Optional<Expression> doExtractExpression(Expression expr, int rewriteChildIndex, boolean rewriteChildToLeft,
            Set<Slot> leftSlots, Set<Slot> rightSlots) {
        Expression target = expr.child(rewriteChildIndex);
        Optional<List<Expression>>  expandTargetOpt = tryExtractCaseWhen(
                target, rewriteChildToLeft, leftSlots, rightSlots);
        if (!expandTargetOpt.isPresent()) {
            return Optional.empty();
        }

        List<Expression> expandTargetExpressions = expandTargetOpt.get();
        if (expandTargetExpressions.size() <= 1) {
            return Optional.empty();
        }

        List<Expression> newChildren = Lists.newArrayList(expr.children());
        List<Expression> disjuncts = Lists.newArrayListWithExpectedSize(expandTargetExpressions.size());
        for (Expression expandTargetExpr : expandTargetExpressions) {
            newChildren.set(rewriteChildIndex, expandTargetExpr);
            disjuncts.add(expr.withChildren(newChildren));
        }

        Expression result = ExpressionUtils.or(disjuncts);
        if (result.getInputSlots().isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(ExpressionUtils.or(result));
    }

    private Optional<List<Expression>> tryExtractCaseWhen(Expression expr, boolean rewriteChildToLeft,
            Set<Slot> leftSlots, Set<Slot> rightSlots) {
        if (isSlotsEmptyOrFrom(expr, rewriteChildToLeft, leftSlots, rightSlots)) {
            return Optional.of(ImmutableList.of(expr));
        } else if (expr instanceof CaseWhen) {
            CaseWhen caseWhen = (CaseWhen) expr;
            List<Expression> resultExpressions
                    = Lists.newArrayListWithExpectedSize(caseWhen.getWhenClauses().size() + 1);
            for (WhenClause whenClause : caseWhen.getWhenClauses()) {
                resultExpressions.add(whenClause.getResult());
            }
            resultExpressions.add(caseWhen.getDefaultValue().orElse(new NullLiteral(caseWhen.getDataType())));
            return checkExpressionSlotsEmptyOrFrom(resultExpressions, rewriteChildToLeft, leftSlots, rightSlots);
        } else if (expr instanceof If) {
            If ifExpr = (If) expr;
            List<Expression> resultExpressions = ImmutableList.of(ifExpr.getTrueValue(), ifExpr.getFalseValue());
            return checkExpressionSlotsEmptyOrFrom(resultExpressions, rewriteChildToLeft, leftSlots, rightSlots);
        } else if (expr instanceof Nvl) {
            Nvl nvlExpr = (Nvl) expr;
            List<Expression> resultExpressions = ImmutableList.of(nvlExpr.left(), nvlExpr.right());
            return checkExpressionSlotsEmptyOrFrom(resultExpressions, rewriteChildToLeft, leftSlots, rightSlots);
        } else if (expr instanceof NullIf) {
            NullIf nullIfExpr = (NullIf) expr;
            List<Expression> resultExpressions
                    = ImmutableList.of(nullIfExpr.left(), new NullLiteral(nullIfExpr.getDataType()));
            return checkExpressionSlotsEmptyOrFrom(resultExpressions, rewriteChildToLeft, leftSlots, rightSlots);
        } else if (!ExpressionUtils.containsCaseWhenLikeType(expr)) {
            return Optional.empty();
        } else {
            int expandChildIndex = -1;
            List<Expression> expandChildExpressions = null;
            List<Expression> newChildren = Lists.newArrayListWithExpectedSize(expr.children().size());
            for (int i = 0; i < expr.children().size(); i++) {
                Expression child = expr.child(i);
                Optional<List<Expression>> childExtractedOpt = tryExtractCaseWhen(
                        child, rewriteChildToLeft, leftSlots, rightSlots);
                if (!childExtractedOpt.isPresent()) {
                    return Optional.empty();
                }
                List<Expression> childExtracted = childExtractedOpt.get();
                if (childExtracted.size() == 1) {
                    Expression newChild = childExtracted.get(0);
                    newChildren.add(newChild);
                } else {
                    // more than one child to expand
                    if (expandChildIndex != -1) {
                        return Optional.empty();
                    }
                    expandChildIndex = i;
                    expandChildExpressions = childExtracted;
                    // will replace the child later, add a placeholder first
                    newChildren.add(child);
                }
            }
            if (expandChildIndex == -1) {
                return Optional.empty();
            }
            List<Expression> resultExpressions = Lists.newArrayListWithExpectedSize(expandChildExpressions.size());
            for (Expression expandChildExpr : expandChildExpressions) {
                newChildren.set(expandChildIndex, expandChildExpr);
                Expression newExpr = expr.withChildren(newChildren);
                resultExpressions.add(newExpr);
            }
            return Optional.of(resultExpressions);
        }
    }

    private Optional<List<Expression>> checkExpressionSlotsEmptyOrFrom(List<Expression> expressions, boolean fromLeft,
            Set<Slot> leftSlots, Set<Slot> rightSlots) {
        for (Expression expr : expressions) {
            if (!isSlotsEmptyOrFrom(expr, fromLeft, leftSlots, rightSlots)) {
                return Optional.empty();
            }
        }
        return Optional.of(expressions);
    }

    private boolean isSlotsEmptyOrFrom(Expression expr, boolean fromLeft, Set<Slot> leftSlots, Set<Slot> rightSlots) {
        Set<Slot> exprSlots = expr.getInputSlots();
        if (fromLeft) {
            return Collections.disjoint(exprSlots, rightSlots);
        } else {
            return Collections.disjoint(exprSlots, leftSlots);
        }
    }

}
