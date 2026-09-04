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

package org.apache.doris.mtmv.ivm.agg;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.ArrayAgg;
import org.apache.doris.nereids.trees.expressions.functions.agg.ArrayAggIf;

/**
 * Processor for ARRAY_AGG(expr).
 *
 * <p>The visible array keeps NULL elements, so polarity columns must be produced by the conditional
 * aggregate {@code array_agg_if} rather than by the NULL-filtering idiom used by MIN/MAX/BITMAP.
 */
class IvmAggArrayAggProcessor extends IvmAggArrayProcessor {
    @Override
    public boolean supportsOriginalFunction(AggregateFunction function) {
        return function instanceof ArrayAgg;
    }

    @Override
    public IvmAggFunctionKind handledFunctionKind() {
        return IvmAggFunctionKind.ARRAY_AGG;
    }

    @Override
    AggregateFunction buildDeltaAggregate(boolean insertSide, Expression elem, Slot dmlFactorSlot,
            IvmAggExpressionBuilder ctx) {
        Expression cond = insertSide ? ctx.factorPositive(dmlFactorSlot)
                : ctx.factorNegative(dmlFactorSlot);
        return new ArrayAggIf(cond, elem);
    }
}
