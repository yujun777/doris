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

import java.util.List;
import java.util.Map;

/**
 * An executable delta plan bundle for one driving base table in an IVM refresh.
 * Produced by {@link IVMDeltaPlanner}, consumed by the delta executor.
 *
 * <p>Each bundle contains the merge/insert/delete plans generated for the driving
 * table's stream changes, along with the runtime subscription for cursor commit
 * and snapshot bindings for consistent reads of other base tables.
 */
public class DeltaPlanBundle {
    private final BaseTableId drivingTable;
    private final StreamSubscription subscription;
    private final Map<BaseTableId, IVMTableSnapshot> tableSnapshots;
    private final List<Plan> mergePlans;

    public DeltaPlanBundle(BaseTableId drivingTable, StreamSubscription subscription,
            Map<BaseTableId, IVMTableSnapshot> tableSnapshots, List<Plan> mergePlans) {
        this.drivingTable = drivingTable;
        this.subscription = subscription;
        this.tableSnapshots = tableSnapshots;
        this.mergePlans = mergePlans;
    }

    public BaseTableId getDrivingTable() {
        return drivingTable;
    }

    public StreamSubscription getSubscription() {
        return subscription;
    }

    public Map<BaseTableId, IVMTableSnapshot> getTableSnapshots() {
        return tableSnapshots;
    }

    public List<Plan> getMergePlans() {
        return mergePlans;
    }

    @Override
    public String toString() {
        return "DeltaPlanBundle{"
                + "drivingTable=" + drivingTable
                + ", mergePlans.size=" + (mergePlans != null ? mergePlans.size() : 0)
                + '}';
    }
}
