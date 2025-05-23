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

import org.apache.doris.common.Config;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;

public class CheckFullScanPartitionNum extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalOlapScan()
                .then(this::checkFullScanPartitionNum)
                .toRule(RuleType.CHECK_FULL_SCAN_PARTITION_NUM);
    }

    private Plan checkFullScanPartitionNum(LogicalOlapScan olapScan) {
        if (olapScan.getManuallySpecifiedPartitions().isEmpty()
                && olapScan.getSelectedPartitionIds().size() >= Config.max_full_scan_partition_num) {
            throw new AnalysisException(String.format("Table %s need scan %s partitions, exceed the max scan partition"
                    + " limit %s (Config.max_full_scan_partition_num, or user need to manually specify scan partitions",
                    olapScan.getTable().getName(), olapScan.getSelectedPartitionIds().size(),
                    Config.max_full_scan_partition_num));
        }

        return olapScan;
    }
}