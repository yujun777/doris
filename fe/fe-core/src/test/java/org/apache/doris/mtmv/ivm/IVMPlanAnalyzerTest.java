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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IVMPlanAnalyzerTest {

    private final IVMPlanAnalyzer analyzer = new IVMPlanAnalyzer();

    @Test
    public void testInnerJoinSupported() {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(1, "t2", 0);

        IVMPlanAnalysis analysis = analyzer.analyze(
                new LogicalPlanBuilder(left).join(right, JoinType.INNER_JOIN, Pair.of(0, 0)).build());

        Assertions.assertEquals(IVMPlanPattern.INNER_JOIN, analysis.getPattern());
        Assertions.assertNull(analysis.getUnsupportedReason());
    }

    @Test
    public void testSelfJoinRejected() {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

        IVMPlanAnalysis analysis = analyzer.analyze(
                new LogicalPlanBuilder(left).join(right, JoinType.INNER_JOIN, Pair.of(0, 0)).build());

        Assertions.assertEquals(IVMPlanPattern.UNSUPPORTED, analysis.getPattern());
        Assertions.assertTrue(analysis.getUnsupportedReason().contains("Repeated base table"));
    }

    @Test
    public void testUnionAllSupported() {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
        SlotReference leftSlot = (SlotReference) left.getOutput().get(0);
        SlotReference rightSlot = (SlotReference) right.getOutput().get(0);

        IVMPlanAnalysis analysis = analyzer.analyze(new LogicalUnion(
                Qualifier.ALL,
                ImmutableList.of(leftSlot),
                ImmutableList.of(
                        ImmutableList.of(leftSlot),
                        ImmutableList.of(rightSlot)),
                ImmutableList.of(),
                true,
                ImmutableList.of(left, right)));

        Assertions.assertEquals(IVMPlanPattern.UNION_ALL_ROOT, analysis.getPattern());
        Assertions.assertNull(analysis.getUnsupportedReason());
    }

    @Test
    public void testUnionAllWithRepeatedBaseTableRejected() {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        SlotReference leftSlot = (SlotReference) left.getOutput().get(0);
        SlotReference rightSlot = (SlotReference) right.getOutput().get(0);

        IVMPlanAnalysis analysis = analyzer.analyze(new LogicalUnion(
                Qualifier.ALL,
                ImmutableList.of(leftSlot),
                ImmutableList.of(
                        ImmutableList.of(leftSlot),
                        ImmutableList.of(rightSlot)),
                ImmutableList.of(),
                true,
                ImmutableList.of(left, right)));

        Assertions.assertEquals(IVMPlanPattern.UNSUPPORTED, analysis.getPattern());
        Assertions.assertTrue(analysis.getUnsupportedReason().contains("Repeated base table"));
    }
}
