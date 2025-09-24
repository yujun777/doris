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

import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Simplify CaseWhen/IF expression.
 */
public class SimplifyCaseWhenLike implements ExpressionPatternRuleFactory {

    public static final SimplifyCaseWhenLike INSTANCE = new SimplifyCaseWhenLike();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(CaseWhen.class)
                        .thenApply(ctx -> simplifyCaseWhen(ctx.expr, ctx.rewriteContext))
                        .toRule(ExpressionRuleType.SIMPLIFY_CASE_WHEN),
                matchesType(If.class)
                        .thenApply(ctx -> simplifyIf(ctx.expr, ctx.rewriteContext))
                        .toRule(ExpressionRuleType.SIMPLIFY_IF)
        );
    }

    private Expression simplifyCaseWhen(CaseWhen caseWhen, ExpressionRewriteContext context) {
        Set<Expression> existWhens = Sets.newHashSet();
        ImmutableList.Builder<WhenClause> newWhenClauses
                = ImmutableList.builderWithExpectedSize(caseWhen.getWhenClauses().size());
        Optional<Expression> newDefaultValueOpt = caseWhen.getDefaultValue();
        for (WhenClause whenClause : caseWhen.getWhenClauses()) {
            Expression operand = whenClause.getOperand();
            if (operand.equals(BooleanLiteral.TRUE)) {
                newDefaultValueOpt = Optional.of(whenClause.getResult());
                break;
            }
            if (operand.equals(BooleanLiteral.FALSE) || operand.isNullLiteral()
                    || existWhens.contains(operand)) {
                continue;
            }

            existWhens.add(operand);
            newWhenClauses.add(whenClause);
        }
        List<WhenClause> newWhenClauseList = newWhenClauses.build();

        boolean allThenEqualsDefault = true;
        Expression newDefaultValue = newDefaultValueOpt.orElse(new NullLiteral(caseWhen.getDataType()));
        for (WhenClause whenClause : newWhenClauseList) {
            if (!whenClause.getResult().equals(newDefaultValue)) {
                allThenEqualsDefault = false;
                break;
            }
        }
        if (allThenEqualsDefault) {
            return TypeCoercionUtils.ensureSameResultType(caseWhen, newDefaultValue, context);
        }
        if (newWhenClauseList.size() == 1) {
            return new If(newWhenClauseList.get(0).getOperand(),
                    TypeCoercionUtils.ensureSameResultType(caseWhen, newWhenClauseList.get(0).getResult(), context),
                    TypeCoercionUtils.ensureSameResultType(caseWhen, newDefaultValue, context));
        }
        if (!newWhenClauseList.equals(caseWhen.getWhenClauses())) {
            return newDefaultValueOpt.map(expression -> new CaseWhen(newWhenClauseList, expression))
                    .orElseGet(() -> new CaseWhen(newWhenClauseList));
        }
        return caseWhen;
    }

    private Expression simplifyIf(If ifExpr, ExpressionRewriteContext context) {
        Expression condition = ifExpr.getCondition();
        Expression trueValue = ifExpr.getTrueValue();
        Expression falseValue = ifExpr.getFalseValue();

        if (condition.equals(BooleanLiteral.TRUE)) {
            return TypeCoercionUtils.ensureSameResultType(ifExpr, trueValue, context);
        } else if (condition.equals(BooleanLiteral.FALSE) || condition.isNullLiteral()) {
            return TypeCoercionUtils.ensureSameResultType(ifExpr, falseValue, context);
        } else if (trueValue.equals(falseValue)) {
            return TypeCoercionUtils.ensureSameResultType(ifExpr, trueValue, context);
        } else {
            return ifExpr;
        }
    }

}
