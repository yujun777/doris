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

import java.util.List;

/**
 * Dispatches IVM delta planning to the appropriate pattern-specific planner
 * based on the {@link IVMPlanPattern} determined by {@link IVMPlanAnalysis}.
 *
 * <p>This is a thin dispatcher that maps patterns to concrete planners,
 * keeping the routing logic out of {@code MTMVTask}.
 *
 * <p>Pattern-to-planner mapping:
 * <ul>
 *   <li>{@code SCAN_ONLY}, {@code FILTER_PROJECT_SCAN} -> {@link IVMScanDeltaPlanner}</li>
 *   <li>{@code INNER_JOIN} -> {@link IVMJoinDeltaPlanner}</li>
 *   <li>{@code AGG_ON_SCAN}, {@code AGG_ON_INNER_JOIN} -> {@link IVMAggDeltaPlanner}</li>
 *   <li>{@code UNION_ALL_ROOT} -> {@link IVMUnionDeltaPlanner}</li>
 * </ul>
 */
public class IVMDeltaPlannerDispatcher {

    private final IVMBaseScanRewriter scanRewriter = new IVMBaseScanRewriter();
    private final IVMDeltaCommandBuilder commandBuilder = new IVMDeltaCommandBuilder();
    private final IVMDeltaPlanner scanPlanner = new IVMScanDeltaPlanner(scanRewriter, commandBuilder);
    private final IVMDeltaPlanner joinPlanner = new IVMJoinDeltaPlanner(scanRewriter, commandBuilder);
    private final IVMDeltaPlanner aggPlanner = new IVMAggDeltaPlanner(scanRewriter, commandBuilder);
    private final IVMDeltaPlanner unionPlanner = new IVMUnionDeltaPlanner(scanRewriter, commandBuilder);

    /**
     * Plans the delta refresh for the given materialized view by dispatching
     * to the appropriate pattern-specific planner.
     *
     * @param mtmv the materialized view to refresh
     * @param context the IVM refresh context (must contain a valid plan analysis)
     * @return list of delta plan bundles, one per driving table with changes
     * @throws AnalysisException if the pattern is unsupported or planning fails
     */
    public List<DeltaPlanBundle> plan(MTMV mtmv, IVMRefreshContext context)
            throws AnalysisException {
        IVMPlanPattern pattern = context.getPlanAnalysis().getPattern();
        return selectPlanner(pattern).plan(mtmv, context);
    }

    /**
     * Selects the concrete planner for the given plan pattern.
     *
     * @throws AnalysisException if the pattern is {@code UNSUPPORTED} or unknown
     */
    private IVMDeltaPlanner selectPlanner(IVMPlanPattern pattern) throws AnalysisException {
        switch (pattern) {
            case SCAN_ONLY:
            case FILTER_PROJECT_SCAN:
                return scanPlanner;
            case INNER_JOIN:
                return joinPlanner;
            case AGG_ON_SCAN:
            case AGG_ON_INNER_JOIN:
                return aggPlanner;
            case UNION_ALL_ROOT:
                return unionPlanner;
            case UNSUPPORTED:
            default:
                throw new AnalysisException(
                        "Unsupported IVM plan pattern: " + pattern);
        }
    }
}
