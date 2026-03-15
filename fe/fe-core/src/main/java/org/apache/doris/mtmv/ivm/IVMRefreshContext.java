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

import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVRefreshSnapshot;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.List;

/**
 * Runtime context dedicated to one IVM refresh execution.
 */
public class IVMRefreshContext {
    private final MTMVRefreshContext baseContext;
    private final Plan rewrittenMvPlan;
    private IVMPlanAnalysis planAnalysis;
    private final List<BaseTableId> baseTableOrder;

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

    public MTMVRefreshSnapshot toRefreshSnapshot() {
        return new MTMVRefreshSnapshot();
    }
}
