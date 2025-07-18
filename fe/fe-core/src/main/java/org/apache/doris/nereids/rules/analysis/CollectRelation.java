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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.View;
import org.apache.doris.common.Pair;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.nereids.CTEContext;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.PlannerHook;
import org.apache.doris.nereids.StatementContext.TableFrom;
import org.apache.doris.nereids.analyzer.UnboundDictionarySink;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.mv.InitMaterializationContextHook;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.UnboundLogicalSink;
import org.apache.doris.nereids.util.RelationUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Rule to bind relations in query plan.
 */
public class CollectRelation implements AnalysisRuleFactory {

    private static final Logger LOG = LogManager.getLogger(CollectRelation.class);

    public CollectRelation() {}

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // should collect table from cte first to fill collect all cte name to avoid collect wrong table.
                logicalCTE()
                        .thenApply(ctx -> {
                            ctx.cascadesContext.setCteContext(collectFromCte(ctx.root, ctx.cascadesContext));
                            return null;
                        })
                        .toRule(RuleType.COLLECT_TABLE_FROM_CTE),
                unboundRelation()
                        .thenApply(this::collectFromUnboundRelation)
                        .toRule(RuleType.COLLECT_TABLE_FROM_RELATION),
                unboundLogicalSink()
                        .thenApply(this::collectFromUnboundSink)
                        .toRule(RuleType.COLLECT_TABLE_FROM_SINK),
                any().whenNot(UnboundRelation.class::isInstance)
                        .whenNot(UnboundTableSink.class::isInstance)
                        .thenApply(this::collectFromAny)
                        .toRule(RuleType.COLLECT_TABLE_FROM_OTHER)
        );
    }

    /**
     * register and store CTEs in CTEContext
     */
    private CTEContext collectFromCte(
            LogicalCTE<Plan> logicalCTE, CascadesContext cascadesContext) {
        CTEContext outerCteCtx = cascadesContext.getCteContext();
        List<LogicalSubQueryAlias<Plan>> aliasQueries = logicalCTE.getAliasQueries();
        for (LogicalSubQueryAlias<Plan> aliasQuery : aliasQueries) {
            // we should use a chain to ensure visible of cte
            LogicalPlan parsedCtePlan = (LogicalPlan) aliasQuery.child();
            CascadesContext innerCascadesCtx = CascadesContext.newContextWithCteContext(
                    cascadesContext, parsedCtePlan, outerCteCtx);
            innerCascadesCtx.newTableCollector().collect();
            LogicalPlan analyzedCtePlan = (LogicalPlan) innerCascadesCtx.getRewritePlan();
            // cteId is not used in CollectTable stage
            CTEId cteId = new CTEId(0);
            LogicalSubQueryAlias<Plan> logicalSubQueryAlias =
                    aliasQuery.withChildren(ImmutableList.of(analyzedCtePlan));
            outerCteCtx = new CTEContext(cteId, logicalSubQueryAlias, outerCteCtx);
            outerCteCtx.setAnalyzedPlan(logicalSubQueryAlias);
        }
        return outerCteCtx;
    }

    private Plan collectFromAny(MatchingContext<Plan> ctx) {
        for (Expression expression : ctx.root.getExpressions()) {
            expression.foreach(e -> {
                if (e instanceof SubqueryExpr) {
                    SubqueryExpr subqueryExpr = (SubqueryExpr) e;
                    CascadesContext subqueryContext = CascadesContext.newContextWithCteContext(
                            ctx.cascadesContext, subqueryExpr.getQueryPlan(), ctx.cteContext);
                    subqueryContext.keepOrShowPlanProcess(ctx.cascadesContext.showPlanProcess(),
                            () -> subqueryContext.newTableCollector().collect());
                    ctx.cascadesContext.addPlanProcesses(subqueryContext.getPlanProcesses());
                }
            });
        }
        return null;
    }

    private Plan collectFromUnboundSink(MatchingContext<UnboundLogicalSink<Plan>> ctx) {
        List<String> nameParts = ctx.root.getNameParts();
        switch (nameParts.size()) {
            case 1:
                // table
                // Use current database name from catalog.
            case 2:
                // db.table
                // Use database name from table name parts.
            case 3:
                // catalog.db.table
                // Use catalog and database name from name parts.
                collectFromUnboundRelation(ctx.cascadesContext, nameParts, TableFrom.INSERT_TARGET, Optional.empty());
                return null;
            default:
                throw new IllegalStateException("Insert target name is invalid.");
        }
    }

    private Plan collectFromUnboundRelation(MatchingContext<UnboundRelation> ctx) {
        List<String> nameParts = ctx.root.getNameParts();
        switch (nameParts.size()) {
            case 1:
                // table
                // Use current database name from catalog.
            case 2:
                // db.table
                // Use database name from table name parts.
            case 3:
                // catalog.db.table
                // Use catalog and database name from name parts.
                collectFromUnboundRelation(ctx.cascadesContext, nameParts, TableFrom.QUERY, Optional.of(ctx.root));
                return null;
            default:
                throw new IllegalStateException("Table name [" + ctx.root.getTableName() + "] is invalid.");
        }
    }

    private void collectFromUnboundRelation(CascadesContext cascadesContext,
            List<String> nameParts, TableFrom tableFrom, Optional<UnboundRelation> unboundRelation) {
        if (nameParts.size() == 1) {
            String tableName = nameParts.get(0);
            // check if it is a CTE's name
            CTEContext cteContext = cascadesContext.getCteContext().findCTEContext(tableName).orElse(null);
            if (cteContext != null) {
                Optional<LogicalPlan> analyzedCte = cteContext.getAnalyzedCTEPlan(tableName);
                if (analyzedCte.isPresent()) {
                    return;
                }
            }
        }

        List<String> tableQualifier = RelationUtil.getQualifierName(cascadesContext.getConnectContext(), nameParts);
        TableIf table;
        if (cascadesContext.getRewritePlan() instanceof UnboundDictionarySink) {
            table = ((UnboundDictionarySink) cascadesContext.getRewritePlan()).getDictionary();
        } else {
            table = cascadesContext.getConnectContext().getStatementContext()
                .getAndCacheTable(tableQualifier, tableFrom, unboundRelation);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("collect table {} from {}", nameParts, tableFrom);
        }
        if (tableFrom == TableFrom.QUERY) {
            collectMTMVCandidates(table, cascadesContext);
        }
        if (tableFrom == TableFrom.INSERT_TARGET) {
            if (!cascadesContext.getStatementContext().getInsertTargetSchema().isEmpty()) {
                LOG.warn("collect insert target table '{}' more than once.", tableQualifier);
            }
            cascadesContext.getStatementContext().getInsertTargetSchema().clear();
            cascadesContext.getStatementContext().getInsertTargetSchema().addAll(table.getFullSchema());
        }
        if (table instanceof View) {
            parseAndCollectFromView(tableQualifier, (View) table, cascadesContext);
        }
    }

    protected void collectMTMVCandidates(TableIf table, CascadesContext cascadesContext) {
        boolean shouldCollect = false;
        for (PlannerHook plannerHook : cascadesContext.getStatementContext().getPlannerHooks()) {
            // only collect when InitMaterializationContextHook exists in planner hooks
            if (plannerHook instanceof InitMaterializationContextHook) {
                shouldCollect = true;
                break;
            }
        }
        if (shouldCollect) {
            boolean isDebugEnabled = LOG.isDebugEnabled();
            Set<MTMV> mtmvSet = Env.getCurrentEnv().getMtmvService().getRelationManager()
                    .getCandidateMTMVs(Lists.newArrayList(new BaseTableInfo(table)));
            if (isDebugEnabled) {
                LOG.debug("table {} related mv set is {}", new BaseTableInfo(table), mtmvSet);
            }
            for (MTMV mtmv : mtmvSet) {
                cascadesContext.getStatementContext().getCandidateMTMVs().add(mtmv);
                cascadesContext.getStatementContext().getMtmvRelatedTables().put(mtmv.getFullQualifiers(), mtmv);
                mtmv.readMvLock();
                try {
                    for (BaseTableInfo baseTableInfo : mtmv.getRelation().getBaseTables()) {
                        if (!baseTableInfo.isValid()) {
                            continue;
                        }
                        if (isDebugEnabled) {
                            LOG.debug("mtmv {} related base table include {}", new BaseTableInfo(mtmv), baseTableInfo);
                        }
                        try {
                            // Collect all base tables and lock them before querying
                            cascadesContext.getStatementContext().getAndCacheTable(baseTableInfo.toList(),
                                    TableFrom.MTMV, Optional.empty());
                        } catch (AnalysisException exception) {
                            LOG.warn("mtmv related base table get err, related table is {}",
                                    baseTableInfo.toList(), exception);
                        }
                    }
                } finally {
                    mtmv.readMvUnlock();
                }
            }
        }
    }

    protected void parseAndCollectFromView(List<String> tableQualifier, View view, CascadesContext parentContext) {
        Pair<String, Long> viewInfo = parentContext.getStatementContext().getAndCacheViewInfo(tableQualifier, view);
        long originalSqlMode = parentContext.getConnectContext().getSessionVariable().getSqlMode();
        parentContext.getConnectContext().getSessionVariable().setSqlMode(viewInfo.second);
        LogicalPlan parsedViewPlan;
        try {
            parsedViewPlan = new NereidsParser().parseSingle(viewInfo.first);
        } finally {
            parentContext.getConnectContext().getSessionVariable().setSqlMode(originalSqlMode);
        }
        if (parsedViewPlan instanceof UnboundResultSink) {
            parsedViewPlan = (LogicalPlan) ((UnboundResultSink<?>) parsedViewPlan).child();
        }
        CascadesContext viewContext = CascadesContext.initContext(
                parentContext.getStatementContext(), parsedViewPlan, PhysicalProperties.ANY);
        viewContext.keepOrShowPlanProcess(parentContext.showPlanProcess(),
                () -> viewContext.newTableCollector().collect());
        parentContext.addPlanProcesses(viewContext.getPlanProcesses());
    }
}
