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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo.RefreshMode;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

/**
 * Tests for REFRESH MATERIALIZED VIEW command parsing with INCREMENTAL/PARTITIONS modes.
 */
public class RefreshMTMVCommandTest {

    private final NereidsParser parser = new NereidsParser();

    private RefreshMTMVInfo extractRefreshInfo(LogicalPlan plan) throws Exception {
        Assert.assertTrue("Parsed plan should be RefreshMTMVCommand",
                plan instanceof RefreshMTMVCommand);
        Field field = RefreshMTMVCommand.class.getDeclaredField("refreshMTMVInfo");
        field.setAccessible(true);
        return (RefreshMTMVInfo) field.get(plan);
    }

    // TC-8-1: REFRESH MV ... INCREMENTAL can be parsed
    @Test
    public void testParseRefreshIncremental() throws Exception {
        LogicalPlan plan = parser.parseSingle("REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");
        RefreshMTMVInfo info = extractRefreshInfo(plan);
        Assert.assertEquals(RefreshMode.INCREMENTAL, info.getRefreshMode());
        Assert.assertTrue(info.getPartitions().isEmpty());
    }

    // TC-8-2: REFRESH MV ... AUTO still parseable
    @Test
    public void testParseRefreshAuto() throws Exception {
        LogicalPlan plan = parser.parseSingle("REFRESH MATERIALIZED VIEW db1.mv1 AUTO");
        RefreshMTMVInfo info = extractRefreshInfo(plan);
        Assert.assertEquals(RefreshMode.AUTO, info.getRefreshMode());
    }

    // TC-8-3: REFRESH MV ... COMPLETE still parseable
    @Test
    public void testParseRefreshComplete() throws Exception {
        LogicalPlan plan = parser.parseSingle("REFRESH MATERIALIZED VIEW db1.mv1 COMPLETE");
        RefreshMTMVInfo info = extractRefreshInfo(plan);
        Assert.assertEquals(RefreshMode.COMPLETE, info.getRefreshMode());
        Assert.assertTrue(info.isComplete());
    }

    // TC-8-4: REFRESH MV ... PARTITIONS parsed as new refresh mode
    @Test
    public void testParseRefreshPartitionsMode() throws Exception {
        LogicalPlan plan = parser.parseSingle("REFRESH MATERIALIZED VIEW db1.mv1 PARTITIONS");
        RefreshMTMVInfo info = extractRefreshInfo(plan);
        Assert.assertEquals(RefreshMode.PARTITIONS, info.getRefreshMode());
        Assert.assertTrue(info.getPartitions().isEmpty());
    }

    // TC-8-extra: REFRESH MV without mode should be rejected (syntax error)
    @Test(expected = Exception.class)
    public void testParseRefreshWithoutModeFails() throws Exception {
        parser.parseSingle("REFRESH MATERIALIZED VIEW db1.mv1");
    }

    // TC-8-extra: REFRESH MV with old-style partitionSpec still works
    @Test
    public void testParseRefreshWithPartitionSpec() throws Exception {
        LogicalPlan plan = parser.parseSingle("REFRESH MATERIALIZED VIEW db1.mv1 PARTITIONS (p1, p2)");
        RefreshMTMVInfo info = extractRefreshInfo(plan);
        Assert.assertEquals(RefreshMode.AUTO, info.getRefreshMode());
        Assert.assertEquals(2, info.getPartitions().size());
    }

    // TC-8-extra: isComplete() backward compat — only true for COMPLETE mode
    @Test
    public void testIsCompleteBackwardCompat() throws Exception {
        LogicalPlan completePlan = parser.parseSingle("REFRESH MATERIALIZED VIEW db1.mv1 COMPLETE");
        RefreshMTMVInfo completeInfo = extractRefreshInfo(completePlan);
        Assert.assertTrue(completeInfo.isComplete());

        LogicalPlan incrementalPlan = parser.parseSingle("REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");
        RefreshMTMVInfo incrementalInfo = extractRefreshInfo(incrementalPlan);
        Assert.assertFalse(incrementalInfo.isComplete());

        LogicalPlan autoPlan = parser.parseSingle("REFRESH MATERIALIZED VIEW db1.mv1 AUTO");
        RefreshMTMVInfo autoInfo = extractRefreshInfo(autoPlan);
        Assert.assertFalse(autoInfo.isComplete());
    }
}
