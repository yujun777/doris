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

package org.apache.doris.nereids.trees.plans.distribute;

import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorkerManager;

import java.util.Objects;

/** DistributeContext */
public class DistributeContext {
    public final DistributedPlanWorkerManager workerManager;
    public final SelectedWorkers selectedWorkers;
    public final boolean isLoadJob;

    public DistributeContext(DistributedPlanWorkerManager workerManager, boolean isLoadJob) {
        this.workerManager = Objects.requireNonNull(workerManager, "workerManager can not be null");
        this.selectedWorkers = new SelectedWorkers(workerManager);
        this.isLoadJob = isLoadJob;
    }
}
