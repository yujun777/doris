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
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Executes the delta plan bundles produced by {@link IVMDeltaPlanner}.
 *
 * <p>The executor does not understand plan tree semantics; it simply consumes
 * {@link DeltaPlanBundle}s in order and executes their merge plans. After each
 * bundle succeeds, it commits the driving table's stream cursor.
 *
 * <p>Execution flow per bundle:
 * <ol>
 *   <li>Prepare a consistent-read context from the bundle's table snapshots</li>
 *   <li>Execute each merge plan (INSERT/DELETE/MERGE) sequentially</li>
 *   <li>Commit the driving table's stream cursor on success</li>
 *   <li>On failure, stop immediately without committing subsequent cursors</li>
 * </ol>
 */
public class IVMDeltaExecutor {

    private static final Logger LOG = LogManager.getLogger(IVMDeltaExecutor.class);

    /**
     * Executes all delta plan bundles for an incremental refresh.
     *
     * <p>Bundles are executed in the order determined by the planner.
     * Each bundle's merge plans are executed sequentially. After all plans
     * in a bundle succeed, the driving table's stream cursor is committed.
     * If any plan fails, execution stops and the exception propagates to
     * trigger fallback.
     *
     * @param mtmv the materialized view being refreshed
     * @param context the IVM refresh context
     * @param bundles the delta plan bundles to execute, in order
     * @throws AnalysisException if execution fails
     */
    public void execute(
            MTMV mtmv,
            IVMRefreshContext context,
            List<DeltaPlanBundle> bundles) throws AnalysisException {
        for (DeltaPlanBundle bundle : bundles) {
            executeBundle(mtmv, context, bundle);
        }
    }

    /**
     * Executes a single delta plan bundle.
     *
     * <p>Prepares the snapshot context, executes all merge plans, then
     * commits the stream cursor on success.
     */
    private void executeBundle(
            MTMV mtmv,
            IVMRefreshContext context,
            DeltaPlanBundle bundle) throws AnalysisException {
        LOG.info("Executing IVM delta bundle for driving table: {}",
                bundle.getDrivingTable());

        for (Plan mergePlan : bundle.getMergePlans()) {
            executePlan(mtmv, context, bundle, mergePlan);
        }

        // All plans in this bundle succeeded — commit the cursor
        commitCursor(bundle);

        LOG.info("Successfully executed IVM delta bundle for driving table: {}",
                bundle.getDrivingTable());
    }

    /**
     * Executes a single merge plan (INSERT/DELETE/MERGE) within a bundle.
     *
     * <p>If the plan is a {@link Command}, it is executed via
     * {@link Command#run}. Otherwise, it is wrapped in a
     * {@link LogicalPlanAdapter} and executed through {@link StmtExecutor}.
     */
    private void executePlan(
            MTMV mtmv,
            IVMRefreshContext context,
            DeltaPlanBundle bundle,
            Plan plan) throws AnalysisException {
        ConnectContext ctx = MTMVPlanUtil.createMTMVContext(
                mtmv, MTMVPlanUtil.DISABLE_RULES_WHEN_RUN_MTMV_TASK);
        StatementContext statementContext = new StatementContext();
        prepareSnapshotContext(statementContext, bundle.getTableSnapshots());
        ctx.setStatementContext(statementContext);

        try {
            if (plan instanceof Command) {
                Command command = (Command) plan;
                StmtExecutor executor = new StmtExecutor(ctx,
                        new LogicalPlanAdapter(command, statementContext));
                ctx.setExecutor(executor);
                ctx.getState().setNereids(true);
                command.run(ctx, executor);
            } else {
                StmtExecutor executor = new StmtExecutor(ctx,
                        LogicalPlanAdapter.of(plan));
                ctx.setExecutor(executor);
                ctx.getState().setNereids(true);
                executor.execute();
            }

            if (ctx.getState().getStateType() != MysqlStateType.OK) {
                throw new AnalysisException(
                        "IVM delta plan execution failed for driving table "
                        + bundle.getDrivingTable() + ": "
                        + ctx.getState().getErrorMessage());
            }
        } catch (AnalysisException e) {
            throw e;
        } catch (Exception e) {
            throw new AnalysisException(
                    "IVM delta plan execution failed for driving table "
                    + bundle.getDrivingTable(), e);
        }
    }

    /**
     * Prepares the statement context with snapshot bindings for consistent reads.
     *
     * <p>For Doris internal tables, snapshots are set via
     * {@link StatementContext#setSnapshot}. For external tables (Iceberg/Paimon),
     * the planner has already written snapshot information into the plan tree's
     * scan parameters, so no additional setup is needed here.
     */
    private void prepareSnapshotContext(
            StatementContext statementContext,
            Map<BaseTableId, IVMTableSnapshot> tableSnapshots) {
        // TODO: convert IVMTableSnapshot to MvccTableInfo/MvccSnapshot pairs
        // and call statementContext.setSnapshot() for each internal table.
        // External table snapshots are already embedded in the plan tree
        // by the scan rewriter.
    }

    /**
     * Commits the stream cursor for the driving table after all plans
     * in the bundle have succeeded.
     */
    private void commitCursor(DeltaPlanBundle bundle) throws AnalysisException {
        StreamSubscription subscription = bundle.getSubscription();
        StreamCursor readableCursor = subscription.getReadableCursor();
        subscription.commitCursor(readableCursor);
        LOG.info("Committed stream cursor for driving table: {}",
                bundle.getDrivingTable());
    }
}
