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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.SupportTableSnapshot;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import java.util.Map;

/**
 * Rewrites Nereids plan trees during IVM delta planning.
 *
 * <p>Responsible for two operations:
 * <ul>
 *   <li>Replacing a driving table's scan node with a stream TVF relation</li>
 *   <li>Binding non-driving base tables to consistent snapshots</li>
 * </ul>
 *
 * <p>The input plan is the output of {@link IVMCreateSqlRewriter}, which wraps
 * scan nodes in {@link org.apache.doris.nereids.trees.plans.logical.LogicalProject}
 * nodes to carry the {@code __ivm_row_id__} column. This rewriter walks through
 * the full plan tree using {@link DefaultPlanRewriter} and matches scan nodes
 * by comparing the scan's bound table metadata against the target {@link BaseTableId}.
 */
public class IVMBaseScanRewriter {

    /**
     * Replaces the driving table's scan node in the MV plan with an
     * {@link UnboundTVFRelation} representing the stream change feed.
     *
     * <p>Walks the plan tree using {@link DefaultPlanRewriter}, finds the
     * {@link LogicalCatalogRelation} whose table matches the driving
     * {@code BaseTableId}, and replaces it with an {@code UnboundTVFRelation}
     * built from the {@link StreamRelationSpec}.
     *
     * @param rewrittenMvPlan the original MV plan tree (from IVMCreateSqlRewriter)
     * @param drivingTable the base table whose scan should be replaced
     * @param relationSpec the stream relation specification for the TVF
     * @return a new plan with the scan replaced
     */
    public Plan replaceDrivingTableWithStream(
            Plan rewrittenMvPlan,
            BaseTableId drivingTable,
            StreamRelationSpec relationSpec) {
        ScanReplacerRewriter rewriter = new ScanReplacerRewriter(drivingTable, relationSpec);
        return rewrittenMvPlan.accept(rewriter, null);
    }

    /**
     * Binds non-driving base table scans to their consistent snapshots.
     *
     * <p>For Doris internal tables, snapshot binding is handled by the executor
     * via {@code StatementContext} — the snapshot information is carried in
     * {@link DeltaPlanBundle#getTableSnapshots()} and applied at execution time.
     * The plan tree itself does not need modification for internal tables.
     *
     * <p>For external tables (Iceberg/Paimon), this may need to rewrite scan
     * nodes to include snapshot parameters in the future.
     *
     * @param replacedPlan the plan after stream replacement
     * @param tableSnapshots snapshot bindings for non-driving tables
     * @return the plan with snapshot bindings applied (currently identity for
     *         internal tables)
     */
    public Plan bindBaseTableSnapshots(
            Plan replacedPlan,
            Map<BaseTableId, IVMTableSnapshot> tableSnapshots) throws AnalysisException {
        try {
            return replacedPlan.accept(new SnapshotBinderRewriter(tableSnapshots), null);
        } catch (SnapshotBindingException e) {
            throw e.analysisException;
        }
    }

    /**
     * Checks whether a {@link LogicalCatalogRelation}'s table matches the
     * given {@link BaseTableId}.
     *
     * <p>Matching is done by comparing catalog name, database name, and table
     * name from the bound table metadata against the BaseTableInfo stored in
     * the BaseTableId. The qualifier may contain 0/1/2 parts depending on how
     * the relation was analyzed, so it is not a stable source for catalog/db
     * resolution here.
     */
    private static boolean matchesTable(
            LogicalCatalogRelation relation, BaseTableId targetTableId) {
        TableIf table = relation.getTable();
        String ctlName = table.getDatabase().getCatalog().getName();
        String dbName = table.getDatabase().getFullName();
        String tableName = table.getName();

        return tableName.equalsIgnoreCase(targetTableId.getTableInfo().getTableName())
                && dbName.equalsIgnoreCase(targetTableId.getTableInfo().getDbName())
                && ctlName.equalsIgnoreCase(targetTableId.getTableInfo().getCtlName());
    }

    /**
     * A {@link DefaultPlanRewriter} that replaces the first matching
     * {@link LogicalCatalogRelation} with an {@link UnboundTVFRelation}.
     */
    private static class ScanReplacerRewriter extends DefaultPlanRewriter<Void> {
        private final BaseTableId targetTableId;
        private final StreamRelationSpec relationSpec;
        private boolean replaced = false;

        ScanReplacerRewriter(BaseTableId targetTableId, StreamRelationSpec relationSpec) {
            this.targetTableId = targetTableId;
            this.relationSpec = relationSpec;
        }

        @Override
        public Plan visit(Plan plan, Void context) {
            if (replaced) {
                return plan;
            }
            if (plan instanceof LogicalCatalogRelation) {
                LogicalCatalogRelation relation = (LogicalCatalogRelation) plan;
                if (matchesTable(relation, targetTableId)) {
                    replaced = true;
                    return new UnboundTVFRelation(
                            StatementScopeIdGenerator.newRelationId(),
                            relationSpec.getFunctionName(),
                            new Properties(relationSpec.getProperties()));
                }
                return relation;
            }
            return super.visit(plan, context);
        }
    }

    private static class SnapshotBinderRewriter extends DefaultPlanRewriter<Void> {
        private final Map<BaseTableId, IVMTableSnapshot> tableSnapshots;

        SnapshotBinderRewriter(Map<BaseTableId, IVMTableSnapshot> tableSnapshots) {
            this.tableSnapshots = tableSnapshots;
        }

        @Override
        public Plan visit(Plan plan, Void context) {
            if (plan instanceof LogicalCatalogRelation) {
                LogicalCatalogRelation relation = (LogicalCatalogRelation) plan;
                for (Map.Entry<BaseTableId, IVMTableSnapshot> entry : tableSnapshots.entrySet()) {
                    if (!matchesTable(relation, entry.getKey())) {
                        continue;
                    }
                    return bindSnapshot(relation, entry.getValue(), entry.getKey());
                }
                return relation;
            }
            return super.visit(plan, context);
        }

        private Plan bindSnapshot(
                LogicalCatalogRelation relation, IVMTableSnapshot tableSnapshot, BaseTableId tableId) {
            if (relation instanceof SupportTableSnapshot) {
                return ((SupportTableSnapshot) relation).withTableSnapshot(tableSnapshot);
            }
            if (!tableSnapshot.asMvccSnapshot().isPresent()) {
                throw new SnapshotBindingException(new AnalysisException(
                        "Plan node does not support snapshot binding for base table: "
                                + tableId + ", relation=" + relation.getClass().getSimpleName()));
            }
            return relation;
        }
    }

    private static class SnapshotBindingException extends RuntimeException {
        private final AnalysisException analysisException;

        SnapshotBindingException(AnalysisException analysisException) {
            super(analysisException);
            this.analysisException = analysisException;
        }
    }
}
