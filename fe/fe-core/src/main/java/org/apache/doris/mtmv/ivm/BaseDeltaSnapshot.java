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

import java.util.Map;

/**
 * Captures the delta context for one driving base table in an IVM refresh cycle.
 * Contains the driving table's stream relation spec and the snapshot bindings
 * for all other base tables.
 */
public class BaseDeltaSnapshot {
    private final BaseTableId drivingTable;
    private final StreamRelationSpec relationSpec;
    private final Map<BaseTableId, IVMTableSnapshot> tableSnapshots;
    private final StreamCapability drivingCapability;

    public BaseDeltaSnapshot(BaseTableId drivingTable, StreamRelationSpec relationSpec,
            Map<BaseTableId, IVMTableSnapshot> tableSnapshots, StreamCapability drivingCapability) {
        this.drivingTable = drivingTable;
        this.relationSpec = relationSpec;
        this.tableSnapshots = tableSnapshots;
        this.drivingCapability = drivingCapability;
    }

    public BaseTableId getDrivingTable() {
        return drivingTable;
    }

    public StreamRelationSpec getRelationSpec() {
        return relationSpec;
    }

    public Map<BaseTableId, IVMTableSnapshot> getTableSnapshots() {
        return tableSnapshots;
    }

    public StreamCapability getDrivingCapability() {
        return drivingCapability;
    }

    @Override
    public String toString() {
        return "BaseDeltaSnapshot{"
                + "drivingTable=" + drivingTable
                + ", relationSpec=" + relationSpec
                + ", tableSnapshots.size=" + (tableSnapshots != null ? tableSnapshots.size() : 0)
                + '}';
    }
}
