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
import org.apache.doris.nereids.trees.plans.Plan;

/**
 * Interface for building write-back plans (INSERT/DELETE/MERGE) that
 * apply delta changes to the materialized view.
 *
 * <p>Implementations construct Nereids command plans targeting the MV table.
 * These plans are later executed by the delta executor.
 */
public interface IVMDeltaCommandBuilder {

    /**
     * Builds an INSERT INTO plan that inserts new rows from the source plan
     * into the materialized view.
     *
     * @param mtmv the target materialized view
     * @param sourcePlan the plan producing rows to insert
     * @return an executable INSERT plan
     */
    Plan buildInsertPlan(MTMV mtmv, Plan sourcePlan) throws AnalysisException;

    /**
     * Builds a DELETE plan that removes rows from the materialized view
     * based on the source plan's results.
     *
     * @param mtmv the target materialized view
     * @param sourcePlan the plan producing rows to delete
     * @return an executable DELETE plan
     */
    Plan buildDeletePlan(MTMV mtmv, Plan sourcePlan) throws AnalysisException;

    /**
     * Builds a MERGE INTO plan that upserts rows into the materialized view.
     * Used for MOW (Merge-On-Write) unique key tables where INSERT+DELETE
     * can be combined into a single MERGE operation.
     *
     * @param mtmv the target materialized view
     * @param sourcePlan the plan producing rows to merge
     * @return an executable MERGE plan
     */
    Plan buildMergePlan(MTMV mtmv, Plan sourcePlan) throws AnalysisException;
}
