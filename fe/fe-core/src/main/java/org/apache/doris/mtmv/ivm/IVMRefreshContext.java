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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVRefreshPartitionSnapshot;
import org.apache.doris.mtmv.MTMVRefreshSnapshot;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.Maps;
import org.apache.commons.collections4.MapUtils;

import java.util.Map;
import java.util.List;
import java.util.Set;

/**
 * Runtime context dedicated to one IVM refresh execution.
 */
public class IVMRefreshContext {
    private final MTMVRefreshContext baseContext;
    private final Plan rewrittenMvPlan;
    private IVMPlanAnalysis planAnalysis;
    private final List<BaseTableId> baseTableOrder;
    private final Map<BaseTableId, IVMTableSnapshot> targetTableSnapshots = Maps.newHashMap();

    public IVMRefreshContext(MTMVRefreshContext baseContext, Plan rewrittenMvPlan,
            List<BaseTableId> baseTableOrder) {
        this.baseContext = baseContext;
        this.rewrittenMvPlan = rewrittenMvPlan;
        this.baseTableOrder = baseTableOrder;
    }

    public MTMVRefreshContext getBaseContext() {
        return baseContext;
    }

    public Plan getRewrittenMvPlan() {
        return rewrittenMvPlan;
    }

    public IVMPlanAnalysis getPlanAnalysis() {
        return planAnalysis;
    }

    public void setPlanAnalysis(IVMPlanAnalysis planAnalysis) {
        this.planAnalysis = planAnalysis;
    }

    public List<BaseTableId> getBaseTableOrder() {
        return baseTableOrder;
    }

    public void recordTargetTableSnapshot(BaseTableId tableId, IVMTableSnapshot tableSnapshot) {
        targetTableSnapshots.put(tableId, tableSnapshot);
    }

    public Map<BaseTableId, IVMTableSnapshot> getTargetTableSnapshots() {
        return targetTableSnapshots;
    }

    public MTMVRefreshSnapshot toRefreshSnapshot() throws AnalysisException {
        if (baseContext == null || baseContext.getMtmv() == null) {
            return new MTMVRefreshSnapshot();
        }
        MTMVRefreshSnapshot currentSnapshot = baseContext.getMtmv().getRefreshSnapshot();
        if (MapUtils.isEmpty(targetTableSnapshots) || MapUtils.isEmpty(baseContext.getPartitionMappings())) {
            return currentSnapshot != null ? currentSnapshot : new MTMVRefreshSnapshot();
        }
        MTMVRelation relation = baseContext.getMtmv().getRelation();
        if (relation == null || relation.getBaseTablesOneLevelAndFromView() == null
                || relation.getBaseTablesOneLevelAndFromView().size() != targetTableSnapshots.size()) {
            return currentSnapshot != null ? currentSnapshot : new MTMVRefreshSnapshot();
        }
        for (Map.Entry<BaseTableId, IVMTableSnapshot> entry : targetTableSnapshots.entrySet()) {
            MTMVSnapshotIf mtmvSnapshot = entry.getValue().asMtmvSnapshot().orElse(null);
            if (mtmvSnapshot == null) {
                return currentSnapshot != null ? currentSnapshot : new MTMVRefreshSnapshot();
            }
            baseContext.getBaseTableSnapshotCache().put(entry.getKey().getTableInfo(), mtmvSnapshot);
        }
        Set<String> partitionNames = baseContext.getPartitionMappings().keySet();
        Map<String, MTMVRefreshPartitionSnapshot> partitionSnapshots = MTMVPartitionUtil.generatePartitionSnapshots(
                baseContext, relation.getBaseTablesOneLevelAndFromView(), partitionNames);
        MTMVRefreshSnapshot refreshSnapshot = new MTMVRefreshSnapshot();
        refreshSnapshot.updateSnapshots(partitionSnapshots, partitionNames);
        return refreshSnapshot;
    }
}
