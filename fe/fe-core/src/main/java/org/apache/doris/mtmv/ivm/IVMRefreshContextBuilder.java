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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.qe.ConnectContext;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * Builds an {@link IVMRefreshContext} for one incremental refresh execution.
 *
 * <p>Responsibilities:
 * <ol>
 *   <li>Read the MV definition and produce the runtime rewritten plan
 *       from {@link MTMVCache}</li>
 *   <li>Read base-table-to-stream bindings from {@link IVMInfo}</li>
 *   <li>Determine a stable base table order from the MV's relation metadata</li>
 *   <li>Construct the {@link IVMRefreshContext} without opening subscriptions
 *       or performing delta planning</li>
 * </ol>
 */
public class IVMRefreshContextBuilder {
    private static final Comparator<BaseTableId> BASE_TABLE_ORDER_COMPARATOR = new Comparator<BaseTableId>() {
        @Override
        public int compare(BaseTableId left, BaseTableId right) {
            BaseTableInfo leftTableInfo = left.getTableInfo();
            BaseTableInfo rightTableInfo = right.getTableInfo();
            int catalogCompare = leftTableInfo.getCtlName().compareTo(rightTableInfo.getCtlName());
            if (catalogCompare != 0) {
                return catalogCompare;
            }
            int dbCompare = leftTableInfo.getDbName().compareTo(rightTableInfo.getDbName());
            if (dbCompare != 0) {
                return dbCompare;
            }
            return leftTableInfo.getTableName().compareTo(rightTableInfo.getTableName());
        }
    };

    /**
     * Builds the IVM refresh context.
     *
     * @param mtmv the materialized view
     * @param ivmInfo the persistent IVM metadata
     * @param baseContext the base refresh context from MTMVTask
     * @return a new IVM refresh context ready for plan analysis and delta planning
     * @throws AnalysisException if the context cannot be built
     */
    public IVMRefreshContext build(
            MTMV mtmv,
            IVMInfo ivmInfo,
            MTMVRefreshContext baseContext) throws AnalysisException {
        Plan rewrittenMvPlan = loadRewrittenMvPlan(mtmv);
        List<BaseTableId> baseTableOrder = buildBaseTableOrder(mtmv, ivmInfo);
        return new IVMRefreshContext(baseContext, rewrittenMvPlan, baseTableOrder);
    }

    /**
     * Loads the rewritten MV plan from the MTMV cache.
     * Uses the original final plan which preserves the logical structure
     * needed for delta planning.
     */
    private Plan loadRewrittenMvPlan(MTMV mtmv) throws AnalysisException {
        try {
            ConnectContext ctx = MTMVPlanUtil.createMTMVContext(
                    mtmv, MTMVPlanUtil.DISABLE_RULES_WHEN_RUN_MTMV_TASK);
            MTMVCache cache = mtmv.getOrGenerateCache(ctx);
            return cache.getOriginalFinalPlan();
        } catch (org.apache.doris.nereids.exceptions.AnalysisException e) {
            throw new AnalysisException(
                    "Failed to load rewritten MV plan for " + mtmv.getName(), e);
        }
    }

    /**
     * Builds a stable, deterministic ordering of base tables for delta planning.
     *
     * <p>The order is derived from the base tables registered in the MV's
     * relation metadata. Stream binding validation happens later in
     * {@link IVMCapabilityChecker}. The order is deterministic (sorted by
     * catalog, database, and table name) to ensure consistent before/after
     * snapshot assignment across refreshes.
     */
    private List<BaseTableId> buildBaseTableOrder(MTMV mtmv, IVMInfo ivmInfo) throws AnalysisException {
        MTMVRelation relation = mtmv.getRelation();
        if (relation == null) {
            throw new AnalysisException("MTMV relation metadata is required for incremental refresh");
        }
        Set<BaseTableInfo> baseTables = relation.getBaseTablesOneLevelAndFromView();
        if (baseTables == null || baseTables.isEmpty()) {
            throw new AnalysisException("Incremental refresh requires at least one base table relation");
        }
        List<BaseTableId> order = new ArrayList<>();
        for (BaseTableInfo tableInfo : baseTables) {
            order.add(new BaseTableId(tableInfo));
        }
        order.sort(BASE_TABLE_ORDER_COMPARATOR);
        return order;
    }
}
