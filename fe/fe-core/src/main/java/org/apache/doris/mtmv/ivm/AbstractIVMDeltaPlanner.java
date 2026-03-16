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
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.Map;

/**
 * Abstract intermediate class for IVM delta planners.
 *
 * <p>Holds references to {@link IVMBaseScanRewriter} and
 * {@link IVMDeltaCommandBuilder}, and delegates scan replacement and
 * snapshot binding to the rewriter. Pattern-specific logic is left
 * to concrete subclasses.
 */
public abstract class AbstractIVMDeltaPlanner extends IVMDeltaPlanner {

    protected final IVMBaseScanRewriter scanRewriter;
    protected final IVMDeltaCommandBuilder commandBuilder;

    protected AbstractIVMDeltaPlanner(
            IVMBaseScanRewriter scanRewriter,
            IVMDeltaCommandBuilder commandBuilder) {
        this.scanRewriter = scanRewriter;
        this.commandBuilder = commandBuilder;
    }

    @Override
    protected final Plan replaceDrivingTableWithStream(
            Plan rewrittenMvPlan,
            BaseTableId drivingTable,
            StreamRelationSpec relationSpec) {
        return scanRewriter.replaceDrivingTableWithStream(
                rewrittenMvPlan, drivingTable, relationSpec);
    }

    @Override
    protected final Plan bindBaseTableSnapshots(
            Plan replacedPlan,
            Map<BaseTableId, IVMTableSnapshot> tableSnapshots) throws AnalysisException {
        return scanRewriter.bindBaseTableSnapshots(replacedPlan, tableSnapshots);
    }

    protected final void validateAppendOnlyStream(BaseDeltaSnapshot baseDeltaSnapshot)
            throws AnalysisException {
        StreamCapability capability = baseDeltaSnapshot.getDrivingCapability();
        if (!capability.isAppendOnly() || capability.isSupportsDelete()
                || capability.isSupportsUpdate() || capability.isSupportsBeforeImage()) {
            throw new AnalysisException("Incremental delta planning currently only supports append-only streams");
        }
    }

    @Override
    protected abstract StreamSubscription openSubscription(IVMStreamRef streamRef)
            throws AnalysisException;
}
