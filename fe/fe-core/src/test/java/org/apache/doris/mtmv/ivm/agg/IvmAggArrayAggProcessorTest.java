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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.ArrayAgg;
import org.apache.doris.nereids.trees.expressions.functions.agg.ArrayAggIf;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayConcat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayExceptAll;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class IvmAggArrayAggProcessorTest extends IvmAggProcessorTestBase {
    @Test
    void testArrayAggBuildsConditionalPolarityDeltaAndMultisetApply() {
        IvmAggArrayAggProcessor processor = new IvmAggArrayAggProcessor();
        Assertions.assertTrue(processor.supportsOriginalFunction(new ArrayAgg(value)));
        Assertions.assertEquals(IvmAggFunctionKind.ARRAY_AGG, processor.handledFunctionKind());
        Assertions.assertTrue(processor.hiddenStateKeys(new ArrayAgg(value)).isEmpty());

        IvmAggTarget target = target(0, IvmAggFunctionKind.ARRAY_AGG, "arr", ArrayType.of(IntegerType.INSTANCE),
                ImmutableMap.of(), valueArg());
        List<NamedExpression> outputs = deltaOutputs(processor, target);
        Assertions.assertEquals(2, outputs.size());
        Assertions.assertTrue(outputs.get(0).child(0) instanceof ArrayAggIf);
        Assertions.assertTrue(outputs.get(1).child(0) instanceof ArrayAggIf);
        Assertions.assertNotEquals(outputs.get(0).getName(), outputs.get(1).getName());

        Map<String, Expression> finalByName = apply(processor, target,
                ImmutableList.of(slot("arr", ArrayType.of(IntegerType.INSTANCE))),
                mappedDeltaSlots(processor, target, outputs),
                slot("delta_group_count", IntegerType.INSTANCE));
        Expression visible = finalByName.get("arr");
        Assertions.assertNotNull(visible);
        Assertions.assertTrue(visible instanceof ArrayExceptAll);
        Assertions.assertTrue(visible.anyMatch(node -> node instanceof ArrayConcat));
        // old, ins and del sides each merge NULL into an empty array
        Assertions.assertEquals(3, visible.collect(node -> node instanceof Coalesce).size());
    }
}
