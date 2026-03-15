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
import org.apache.doris.mtmv.MTMVRefreshContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;

/**
 * Orchestrator for materialized view incremental refresh (IVM).
 *
 * <p>{@code IVMManager} is the entry point called from {@code MTMVTask}'s
 * incremental branch. It coordinates the full refresh lifecycle:
 * <ol>
 *   <li>Check if a previous incremental refresh is still in progress</li>
 *   <li>Build the {@link IVMRefreshContext}</li>
 *   <li>Analyze the plan and check incremental capability</li>
 *   <li>Dispatch to the appropriate delta planner</li>
 *   <li>Execute the resulting delta plan bundles</li>
 *   <li>Update MTMV state on success, or signal fallback on failure</li>
 * </ol>
 *
 * <p>{@code IVMManager} only orchestrates; it does not perform plan rewriting
 * or delta computation itself.
 */
public class IVMManager {

    private static final Logger LOG = LogManager.getLogger(IVMManager.class);

    private final IVMCapabilityChecker capabilityChecker;
    private final IVMPlanAnalyzer planAnalyzer;
    private final IVMRefreshContextBuilder refreshContextBuilder;
    private final IVMDeltaPlannerDispatcher deltaPlannerDispatcher;
    private final IVMDeltaExecutor deltaExecutor;

    public IVMManager(
            IVMCapabilityChecker capabilityChecker,
            IVMPlanAnalyzer planAnalyzer,
            IVMRefreshContextBuilder refreshContextBuilder,
            IVMDeltaPlannerDispatcher deltaPlannerDispatcher,
            IVMDeltaExecutor deltaExecutor) {
        this.capabilityChecker = capabilityChecker;
        this.planAnalyzer = planAnalyzer;
        this.refreshContextBuilder = refreshContextBuilder;
        this.deltaPlannerDispatcher = deltaPlannerDispatcher;
        this.deltaExecutor = deltaExecutor;
    }

    /**
     * Attempts an incremental refresh for the given materialized view.
     *
     * <p>Returns {@link Optional#empty()} on success. On failure, returns
     * a {@link FallbackReason} so that {@code MTMVTask} can decide whether
     * to fall back to partition or full refresh.
     *
     * @param mtmv the materialized view to refresh
     * @param baseContext the base refresh context from MTMVTask
     * @return empty if incremental refresh succeeded, or the fallback reason
     */
    public Optional<FallbackReason> ivmRefresh(MTMV mtmv, MTMVRefreshContext baseContext) {
        IVMInfo ivmInfo = mtmv.getIvmInfo();

        // Guard: reject if a previous incremental refresh did not complete
        if (ivmInfo.isInIncrementalRefresh()) {
            LOG.warn("Previous incremental refresh for MV {} did not complete, "
                    + "falling back", mtmv.getName());
            return Optional.of(FallbackReason.PREVIOUS_RUN_INCOMPLETE);
        }

        // Phase 1: build IVM refresh context
        IVMRefreshContext context;
        try {
            context = refreshContextBuilder.build(mtmv, ivmInfo, baseContext);
        } catch (AnalysisException e) {
            LOG.warn("Failed to build IVM refresh context for MV {}: {}",
                    mtmv.getName(), e.getMessage());
            return Optional.of(FallbackReason.INCREMENTAL_EXECUTION_FAILED);
        }

        // Phase 2: analyze plan pattern
        IVMPlanAnalysis analysis = planAnalyzer.analyze(context.getRewrittenMvPlan());
        context.setPlanAnalysis(analysis);

        if (analysis.getPattern() == IVMPlanPattern.UNSUPPORTED) {
            LOG.info("Plan pattern unsupported for incremental refresh of MV {}: {}",
                    mtmv.getName(), analysis.getUnsupportedReason());
            return Optional.of(FallbackReason.PLAN_PATTERN_UNSUPPORTED);
        }

        // Phase 3: check incremental capability
        IVMCapabilityResult capability = capabilityChecker.check(mtmv, context);
        if (!capability.isIncremental()) {
            LOG.info("Incremental refresh not viable for MV {}: {} - {}",
                    mtmv.getName(), capability.getFallbackReason(),
                    capability.getDetailMessage());
            return Optional.of(capability.getFallbackReason());
        }

        // Phase 4: mark in-progress, plan, execute, finalize
        ivmInfo.setInIncrementalRefresh(true);
        // TODO: writeEditLog(mtmv) to persist in-progress state
        try {
            List<DeltaPlanBundle> bundles = deltaPlannerDispatcher.plan(mtmv, context);
            deltaExecutor.execute(mtmv, context, bundles);

            // Success: update refresh snapshot and clear in-progress flag
            mtmv.setRefreshSnapshot(context.toRefreshSnapshot());
            ivmInfo.setInIncrementalRefresh(false);
            // TODO: writeEditLog(mtmv) to persist completed state
            LOG.info("Incremental refresh succeeded for MV {}", mtmv.getName());
            return Optional.empty();

        } catch (Exception e) {
            LOG.warn("Incremental refresh failed for MV {}: {}",
                    mtmv.getName(), e.getMessage(), e);
            ivmInfo.setInIncrementalRefresh(false);
            return Optional.of(FallbackReason.INCREMENTAL_EXECUTION_FAILED);
        }
    }
}
