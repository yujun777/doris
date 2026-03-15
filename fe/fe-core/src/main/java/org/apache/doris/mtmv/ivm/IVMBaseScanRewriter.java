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

import org.apache.doris.nereids.trees.plans.Plan;

import java.util.Map;

/**
 * Interface for rewriting Nereids plan trees during IVM delta planning.
 *
 * <p>Responsible for two operations:
 * <ul>
 *   <li>Replacing a driving table's scan node with a stream TVF relation</li>
 *   <li>Binding non-driving base tables to consistent snapshots</li>
 * </ul>
 */
public interface IVMBaseScanRewriter {

    /**
     * Replaces the driving table's scan node in the MV plan with an
     * {@code UnboundTVFRelation} representing the stream change feed.
     *
     * @param rewrittenMvPlan the original MV plan tree
     * @param drivingTable the base table whose scan should be replaced
     * @param relationSpec the stream relation specification for the TVF
     * @return a new plan with the scan replaced
     */
    Plan replaceDrivingTableWithStream(
            Plan rewrittenMvPlan,
            BaseTableId drivingTable,
            StreamRelationSpec relationSpec);

    /**
     * Binds non-driving base table scans to their consistent snapshots.
     *
     * <p>For external tables (Iceberg/Paimon), this may rewrite scan nodes
     * to include snapshot parameters. For Doris internal tables, snapshot
     * information is saved in {@link DeltaPlanBundle} for the executor
     * to set via {@code StatementContext.setSnapshot()}.
     *
     * @param replacedPlan the plan after stream replacement
     * @param tableSnapshots snapshot bindings for non-driving tables
     * @return the plan with snapshot bindings applied
     */
    Plan bindBaseTableSnapshots(
            Plan replacedPlan,
            Map<BaseTableId, IVMTableSnapshot> tableSnapshots);
}
