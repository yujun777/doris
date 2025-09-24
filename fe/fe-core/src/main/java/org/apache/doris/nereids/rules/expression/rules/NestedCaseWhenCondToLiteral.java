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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * For nested CaseWhen/IF expression, replace the inner CaseWhen/IF condition with TRUE/FALSE literal
 * when the condition also exists in the outer CaseWhen/IF conditions.
 */
public class NestedCaseWhenCondToLiteral implements ExpressionPatternRuleFactory {

    public static final NestedCaseWhenCondToLiteral INSTANCE = new NestedCaseWhenCondToLiteral();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                root(Expression.class)
                        .when(this::needRewrite)
                        .then(this::rewrite)
                        .toRule(ExpressionRuleType.NESTED_CASE_WHEN_COND_TO_LITERAL)
        );
    }

    private boolean needRewrite(Expression expression) {
        return expression.containsType(CaseWhen.class, If.class);
    }

    private Expression rewrite(Expression expression) {
        return expression.accept(new NestCaseLikeCondReplacer(), null);
    }

    private static class NestCaseLikeCondReplacer extends DefaultExpressionRewriter<Void> {

        private final Set<Expression> trueConditions = Sets.newHashSet();
        private final Set<Expression> falseConditions = Sets.newHashSet();

        @Override
        public Expression visit(Expression expr, Void context) {
            if (!INSTANCE.needRewrite(expr)) {
                return expr;
            }
            ImmutableList.Builder<Expression> newChildren
                    = ImmutableList.builderWithExpectedSize(expr.arity());
            boolean hasNewChildren = false;
            for (Expression child : expr.children()) {
                Expression newChild = child.accept(this, null);
                if (newChild != child) {
                    hasNewChildren = true;
                }
                newChildren.add(newChild);
            }
            return hasNewChildren ? expr.withChildren(newChildren.build()) : expr;
        }

        @Override
        public Expression visitCaseWhen(CaseWhen caseWhen, Void context) {
            ImmutableList.Builder<WhenClause> newWhenClauses
                    = ImmutableList.builderWithExpectedSize(caseWhen.arity());
            List<Expression> newAddConds = Lists.newArrayListWithExpectedSize(caseWhen.arity());
            boolean hasNewChildren = false;
            Set<Expression> uniqueConditions = Sets.newHashSet();
            for (WhenClause whenClause : caseWhen.getWhenClauses()) {
                Expression oldCondition = whenClause.getOperand();
                if (!uniqueConditions.add(oldCondition)) {
                    // remove duplicated when condition
                    hasNewChildren = true;
                    continue;
                }
                Pair<Expression, Boolean> replaceResult = replaceCondition(oldCondition);
                Expression newCondition = replaceResult.first;
                boolean addCond = replaceResult.second;
                if (addCond) {
                    trueConditions.add(oldCondition);
                    newAddConds.add(oldCondition);
                }
                Expression newResult = whenClause.getResult().accept(this, null);
                if (addCond) {
                    trueConditions.remove(oldCondition);
                    falseConditions.add(oldCondition);
                }
                if (whenClause.getOperand() != newCondition || whenClause.getResult() != newResult) {
                    hasNewChildren = true;
                    newWhenClauses.add(new WhenClause(newCondition, newResult));
                } else {
                    newWhenClauses.add(whenClause);
                }
            }
            Expression oldDefaultValue = caseWhen.getDefaultValue().orElse(null);
            Expression newDefaultValue = oldDefaultValue;
            if (newDefaultValue != null) {
                newDefaultValue = oldDefaultValue.accept(this, null);
                if (oldDefaultValue != newDefaultValue) {
                    hasNewChildren = true;
                }
            }
            for (Expression newAddCond : newAddConds) {
                falseConditions.remove(newAddCond);
            }
            if (hasNewChildren) {
                return newDefaultValue != null
                        ? new CaseWhen(newWhenClauses.build(), newDefaultValue)
                        : new CaseWhen(newWhenClauses.build());
            } else {
                return caseWhen;
            }
        }

        @Override
        public Expression visitIf(If ifExpr, Void context) {
            Expression oldCondition = ifExpr.getCondition();
            Pair<Expression, Boolean> replaceResult = replaceCondition(oldCondition);
            Expression newCondition = replaceResult.first;
            boolean addCond = replaceResult.second;
            if (addCond) {
                trueConditions.add(oldCondition);
            }
            Expression newTrueValue = ifExpr.getTrueValue().accept(this, null);
            if (addCond) {
                trueConditions.remove(oldCondition);
                falseConditions.add(oldCondition);
            }
            Expression newFalseValue = ifExpr.getFalseValue().accept(this, null);
            if (addCond) {
                falseConditions.remove(oldCondition);
            }
            if (newCondition != oldCondition
                    || newTrueValue != ifExpr.getTrueValue()
                    || newFalseValue != ifExpr.getFalseValue()) {
                return new If(newCondition, newTrueValue, newFalseValue);
            } else {
                return ifExpr;
            }
        }

        // return newCondition + need add condition to trueConditions/falseConditions
        private Pair<Expression, Boolean> replaceCondition(Expression condition) {
            if (condition.isLiteral()) {
                return Pair.of(condition, false);
            } else if (trueConditions.contains(condition)) {
                return Pair.of(BooleanLiteral.TRUE, false);
            } else if (falseConditions.contains(condition)) {
                return Pair.of(BooleanLiteral.FALSE, false);
            } else {
                return Pair.of(condition.accept(this, null), true);
            }
        }
    }
}
