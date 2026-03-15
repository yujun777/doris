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
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.analyzer.UnboundTableSinkCreator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * Builds write-back command plans (INSERT/DELETE/MERGE) that apply delta
 * changes to the materialized view.
 *
 * <p>Each method wraps the given source plan with an appropriate
 * {@link org.apache.doris.nereids.analyzer.UnboundTableSink} targeting the
 * MV table, then constructs an {@link InsertIntoTableCommand} for execution.
 *
 * <p>The source plan is the delta plan produced by the delta planner — it
 * reads from stream TVFs and/or snapshot-bound base tables and produces the
 * rows to be written to the MV.
 */
public class IVMDeltaCommandBuilder {

    /**
     * Builds an INSERT INTO command plan that inserts new rows from the
     * source plan into the materialized view.
     *
     * @param mtmv the target materialized view
     * @param sourcePlan the plan producing rows to insert
     * @return an executable INSERT command plan
     */
    public Plan buildInsertPlan(MTMV mtmv, Plan sourcePlan) throws AnalysisException {
        List<String> nameParts = getMvNameParts(mtmv);
        LogicalPlan sinkPlan = (LogicalPlan) UnboundTableSinkCreator.createUnboundTableSink(
                nameParts,
                ImmutableList.of(),
                ImmutableList.of(),
                false,
                ImmutableList.of(),
                false,
                TPartialUpdateNewRowPolicy.APPEND,
                DMLCommandType.INSERT,
                (LogicalPlan) sourcePlan);
        return new InsertIntoTableCommand(sinkPlan,
                Optional.empty(), Optional.empty(), Optional.empty());
    }

    /**
     * Builds a DELETE command plan that removes rows from the materialized
     * view based on the source plan's results.
     *
     * <p>For unique key MV tables with merge-on-write, DELETE is internally
     * handled as a special INSERT with {@link DMLCommandType#DELETE} that
     * marks rows for deletion.
     *
     * @param mtmv the target materialized view
     * @param sourcePlan the plan producing rows to delete
     * @return an executable DELETE command plan
     */
    public Plan buildDeletePlan(MTMV mtmv, Plan sourcePlan) throws AnalysisException {
        List<String> nameParts = getMvNameParts(mtmv);
        LogicalPlan sinkPlan = (LogicalPlan) UnboundTableSinkCreator.createUnboundTableSink(
                nameParts,
                ImmutableList.of(),
                ImmutableList.of(),
                false,
                ImmutableList.of(),
                false,
                TPartialUpdateNewRowPolicy.APPEND,
                DMLCommandType.DELETE,
                (LogicalPlan) sourcePlan);
        return new InsertIntoTableCommand(sinkPlan,
                Optional.empty(), Optional.empty(), Optional.empty());
    }

    /**
     * Builds a MERGE command plan that upserts rows into the materialized
     * view. Used for aggregate patterns where matched groups get their
     * values updated and unmatched groups get inserted.
     *
     * <p>For unique key MV tables, this is implemented as a partial update
     * INSERT that naturally performs upsert semantics keyed by the primary
     * key ({@code __ivm_row_id__}).
     *
     * @param mtmv the target materialized view
     * @param sourcePlan the plan producing rows to merge
     * @return an executable MERGE command plan
     */
    public Plan buildMergePlan(MTMV mtmv, Plan sourcePlan) throws AnalysisException {
        // For unique key tables with merge-on-write, INSERT with the same
        // primary key (__ivm_row_id__) naturally performs upsert.
        // This is equivalent to MERGE: matched rows are updated,
        // unmatched rows are inserted.
        List<String> nameParts = getMvNameParts(mtmv);
        LogicalPlan sinkPlan = (LogicalPlan) UnboundTableSinkCreator.createUnboundTableSink(
                nameParts,
                ImmutableList.of(),
                ImmutableList.of(),
                false,
                ImmutableList.of(),
                true,
                TPartialUpdateNewRowPolicy.APPEND,
                DMLCommandType.INSERT,
                (LogicalPlan) sourcePlan);
        return new InsertIntoTableCommand(sinkPlan,
                Optional.empty(), Optional.empty(), Optional.empty());
    }

    /**
     * Returns the qualified name parts {@code [catalog, database, table]}
     * for the given materialized view.
     */
    private List<String> getMvNameParts(MTMV mtmv) {
        return ImmutableList.of(
                InternalCatalog.INTERNAL_CATALOG_NAME,
                mtmv.getQualifiedDbName(),
                mtmv.getName());
    }
}
