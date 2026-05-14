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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.info.TableNameInfoUtils;
import org.apache.doris.mtmv.ivm.IvmAggMeta.AggTarget;
import org.apache.doris.mtmv.ivm.IvmAggMeta.AggType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UniqueFunction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Builds the stable IVM maintenance-layout signature.
 */
public class IvmPlanSignatureGenerator {
    public static final int CURRENT_VERSION = 1;
    private static final String HEADER = "IVM_LAYOUT_SIGNATURE_V" + CURRENT_VERSION;

    private final CanonicalPlanVisitor canonicalPlanVisitor = new CanonicalPlanVisitor();

    public IvmPlanSignatureGenerator() {
    }

    public IvmPlanSignature generate(IvmNormalizeResult normalizeResult) {
        Plan normalizedPlan = normalizeResult.getNormalizedPlan();
        CanonicalNode root = CanonicalNode.node("ROOT")
                .field("output", canonicalOutput(normalizedPlan.getOutput()))
                .field("plan", canonicalPlan(normalizedPlan, normalizeResult));
        String canonical = HEADER + "\n" + root.encoded();
        return new IvmPlanSignature(canonical, sha256(canonical));
    }

    private CanonicalNode canonicalPlan(Plan plan, IvmNormalizeResult normalizeResult) {
        return plan.accept(canonicalPlanVisitor, normalizeResult);
    }

    private CanonicalNode canonicalProject(LogicalProject<?> project, IvmNormalizeResult normalizeResult) {
        return CanonicalNode.node("PROJECT")
                .field("distinct", project.isDistinct())
                .field("outputs", canonicalNamedExpressions(project.getProjects()))
                .field("child", canonicalPlan(project.child(), normalizeResult));
    }

    private CanonicalNode canonicalAggregate(LogicalAggregate<?> agg, IvmNormalizeResult normalizeResult) {
        CanonicalNode node = CanonicalNode.node("AGG")
                .field("groupBy", canonicalExpressions(agg.getGroupByExpressions()))
                .field("outputs", canonicalNamedExpressions(agg.getOutputExpressions()))
                .field("child", canonicalPlan(agg.child(), normalizeResult));
        if (normalizeResult.getAggMeta() != null) {
            node.field("aggMeta", canonicalAggMeta(normalizeResult.getAggMeta()));
        }
        return node;
    }

    private CanonicalNode canonicalAggMeta(IvmAggMeta aggMeta) {
        CanonicalList targets = CanonicalList.list();
        for (AggTarget target : aggMeta.getAggTargets()) {
            targets.add(canonicalAggTarget(target));
        }
        return CanonicalNode.node("AGG_META")
                .field("scalar", aggMeta.isScalarAgg())
                .field("groupKeys", canonicalSlots(aggMeta.getGroupKeySlots()))
                .field("rowId", aggMeta.isScalarAgg() ? "const(0)" : "hash(groupKeys)")
                .field("groupCount", canonicalSlot(aggMeta.getGroupCountSlot()))
                .field("targets", targets);
    }

    private CanonicalNode canonicalAggTarget(AggTarget target) {
        return CanonicalNode.node("AGG_TARGET")
                .field("ordinal", target.getOrdinal())
                .field("aggType", target.getAggType())
                .field("visible", canonicalSlot(target.getVisibleSlot()))
                .field("args", canonicalExpressions(target.getExprArgs()))
                .field("hidden", canonicalHiddenStates(target.getHiddenStateSlots()));
    }

    private CanonicalList canonicalHiddenStates(Map<AggType, Slot> hiddenStateSlots) {
        List<Map.Entry<AggType, Slot>> entries = new ArrayList<>(hiddenStateSlots.entrySet());
        entries.sort(new Comparator<Map.Entry<AggType, Slot>>() {
            @Override
            public int compare(Map.Entry<AggType, Slot> left, Map.Entry<AggType, Slot> right) {
                return left.getKey().name().compareTo(right.getKey().name());
            }
        });
        CanonicalList states = CanonicalList.list();
        for (Map.Entry<AggType, Slot> entry : entries) {
            states.add(CanonicalNode.node("HIDDEN_STATE")
                    .field("type", entry.getKey().name())
                    .field("slot", canonicalSlot(entry.getValue())));
        }
        return states;
    }

    private CanonicalNode canonicalScan(LogicalOlapScan scan) {
        OlapTable table = scan.getTable();
        return CanonicalNode.node("SCAN")
                .field("table", tableIdentity(table, scan.getQualifier()))
                .field("keysType", table.getKeysType())
                .field("rowId", rowIdInfo(scan))
                .field("outputs", canonicalSlots(scan.getOutput()));
    }

    private CanonicalValue rowIdInfo(LogicalOlapScan scan) {
        OlapTable table = scan.getTable();
        if (table.getKeysType() == KeysType.DUP_KEYS) {
            return canonicalValue("uuid_numeric()");
        }
        if (table.getKeysType() == KeysType.UNIQUE_KEYS || table.getKeysType() == KeysType.AGG_KEYS) {
            return CanonicalNode.node("ROW_ID_HASH")
                    .field("keys", keyColumns(table, scan));
        }
        return CanonicalNode.node("UNSUPPORTED_ROW_ID")
                .field("keysType", table.getKeysType());
    }

    private CanonicalList keyColumns(OlapTable table, LogicalOlapScan scan) {
        Set<String> keyColNames = Sets.newHashSet();
        for (Column column : table.getBaseSchemaKeyColumns()) {
            keyColNames.add(column.getName());
        }
        CanonicalList keySlots = CanonicalList.list();
        for (Slot slot : scan.getOutput()) {
            if (keyColNames.contains(slot.getName())) {
                keySlots.add(canonicalSlot(slot));
            }
        }
        return keySlots;
    }

    private CanonicalNode canonicalJoin(LogicalJoin<?, ?> join, IvmNormalizeResult normalizeResult) {
        return CanonicalNode.node("JOIN")
                .field("left", canonicalPlan(join.left(), normalizeResult))
                .field("right", canonicalPlan(join.right(), normalizeResult));
    }

    private CanonicalNode canonicalUnion(LogicalUnion union, IvmNormalizeResult normalizeResult) {
        if (union.getQualifier() != Qualifier.ALL) {
            return CanonicalNode.node("UNION")
                    .field("qualifier", union.getQualifier());
        }
        CanonicalList arms = CanonicalList.list();
        for (int i = 0; i < union.children().size(); i++) {
            arms.add(CanonicalNode.node("UNION_ARM")
                    .field("index", i)
                    .field("plan", canonicalPlan(union.child(i), normalizeResult)));
        }
        CanonicalList childOutputs = CanonicalList.list();
        for (List<SlotReference> output : union.getRegularChildrenOutputs()) {
            childOutputs.add(canonicalSlots(output));
        }
        return CanonicalNode.node("UNION")
                .field("rowId", "hash(armIndex,child.rowId)")
                .field("outputs", canonicalNamedExpressions(union.getOutputs()))
                .field("childOutputs", childOutputs)
                .field("arms", arms);
    }

    private CanonicalList canonicalOutput(List<Slot> output) {
        CanonicalList columns = CanonicalList.list();
        for (int i = 0; i < output.size(); i++) {
            Slot slot = output.get(i);
            columns.add(CanonicalNode.node("COLUMN")
                    .field("ordinal", i)
                    .field("name", slot.getName())
                    .field("type", type(slot))
                    .field("nullable", slot.nullable())
                    .field("ivm", Column.IVM_ROW_ID_COL.equals(slot.getName())
                            || IvmUtil.isIvmHiddenColumn(slot.getName())));
        }
        return columns;
    }

    private CanonicalList canonicalNamedExpressions(List<? extends NamedExpression> expressions) {
        CanonicalList result = CanonicalList.list();
        for (int i = 0; i < expressions.size(); i++) {
            NamedExpression expression = expressions.get(i);
            result.add(CanonicalNode.node("NAMED_EXPR")
                    .field("ordinal", i)
                    .field("name", expression.getName())
                    .field("expr", canonicalExpressionNode(expression))
                    .field("slot", canonicalSlot(expression.toSlot())));
        }
        return result;
    }

    private CanonicalList canonicalExpressions(List<? extends Expression> expressions) {
        CanonicalList result = CanonicalList.list();
        for (Expression expression : expressions) {
            result.add(canonicalExpressionNode(expression));
        }
        return result;
    }

    private CanonicalList canonicalSlots(List<? extends Slot> slots) {
        CanonicalList result = CanonicalList.list();
        for (Slot slot : slots) {
            result.add(canonicalSlot(slot));
        }
        return result;
    }

    @VisibleForTesting
    String canonicalExpression(Expression expression) {
        return canonicalExpressionNode(expression).encoded();
    }

    private CanonicalNode canonicalExpressionNode(Expression expression) {
        if (expression instanceof Alias) {
            return CanonicalNode.node("ALIAS")
                    .field("child", canonicalExpressionNode(((Alias) expression).child()));
        }
        if (expression instanceof Slot) {
            return canonicalSlot((Slot) expression);
        }
        if (expression instanceof Literal) {
            Literal literal = (Literal) expression;
            return CanonicalNode.node("LITERAL")
                    .field("type", type(literal))
                    .field("value", String.valueOf(literal.getValue()));
        }
        if (expression instanceof Cast) {
            Cast cast = (Cast) expression;
            return CanonicalNode.node("CAST")
                    .field("class", expression.getClass().getSimpleName())
                    .field("explicit", cast.isExplicitType())
                    .field("child", canonicalExpressionNode(cast.child()))
                    .field("targetType", type(expression));
        }
        if (expression instanceof AggregateFunction) {
            AggregateFunction function = (AggregateFunction) expression;
            return CanonicalNode.node("AGG_FUNC")
                    .field("name", function.getName().toUpperCase(Locale.ROOT))
                    .field("distinct", function.isDistinct())
                    .field("skew", function.isSkew())
                    .field("args", canonicalExpressions(function.children()))
                    .field("type", type(expression))
                    .field("nullable", expression.nullable());
        }
        if (expression instanceof BoundFunction) {
            BoundFunction function = (BoundFunction) expression;
            return CanonicalNode.node("FUNC")
                    .field("name", function.getName().toUpperCase(Locale.ROOT))
                    .field("unique", function instanceof UniqueFunction)
                    .field("args", canonicalExpressions(function.children()))
                    .field("type", type(expression))
                    .field("nullable", expression.nullable());
        }
        return CanonicalNode.node("EXPR")
                .field("class", expression.getClass().getSimpleName())
                .field("children", canonicalExpressions(expression.children()))
                .field("type", type(expression))
                .field("nullable", expression.nullable());
    }

    private CanonicalNode canonicalSlot(Slot slot) {
        if (slot instanceof SlotReference) {
            SlotReference slotReference = (SlotReference) slot;
            CanonicalNode node = CanonicalNode.node("SLOT")
                    .field("type", type(slot))
                    .field("nullable", slot.nullable());
            if (slotReference.getOriginalTable().isPresent() && slotReference.getOriginalColumn().isPresent()) {
                TableIf table = slotReference.getOriginalTable().get();
                String columnName = slotReference.getOriginalColumn().get().getName();
                return node.field("identity", tableIdentity(table, slotReference.getQualifier()) + "." + columnName)
                        .field("subPath", canonicalSubPath(slotReference));
            }
            return node.field("qualifier", canonicalQualifier(slotReference.getQualifier()))
                    .field("name", slot.getName())
                    .field("subPath", canonicalSubPath(slotReference));
        }
        return CanonicalNode.node("SLOT")
                .field("name", slot.getName())
                .field("type", type(slot))
                .field("nullable", slot.nullable());
    }

    private String tableIdentity(TableIf table, List<String> fallbackQualifier) {
        TableNameInfo tableNameInfo = TableNameInfoUtils.fromTableOrNull(table);
        if (tableNameInfo != null) {
            return tableNameInfo.toString();
        }
        if (!fallbackQualifier.isEmpty()) {
            return String.join(".", fallbackQualifier) + "." + table.getName();
        }
        if (table instanceof Table && StringUtils.isNotEmpty(((Table) table).getQualifiedDbName())) {
            return ((Table) table).getQualifiedDbName() + "." + table.getName();
        }
        return table.getName();
    }

    private CanonicalList canonicalQualifier(List<String> qualifier) {
        CanonicalList result = CanonicalList.list();
        for (String item : qualifier) {
            result.add(item);
        }
        return result;
    }

    private CanonicalList canonicalSubPath(SlotReference slotReference) {
        CanonicalList result = CanonicalList.list();
        for (String item : slotReference.getSubPath()) {
            result.add(item);
        }
        return result;
    }

    private String type(Expression expression) {
        return expression.getDataType().toSql();
    }

    private static String sha256(String canonical) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(canonical.getBytes(StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                builder.append(String.format("%02x", b));
            }
            return builder.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest is unavailable", e);
        }
    }

    private static CanonicalValue canonicalValue(Object value) {
        if (value instanceof CanonicalValue) {
            return (CanonicalValue) value;
        }
        return new CanonicalScalar(String.valueOf(value));
    }

    private interface CanonicalValue {
        void encodeTo(StringBuilder builder);

        default String encoded() {
            StringBuilder builder = new StringBuilder();
            encodeTo(builder);
            return builder.toString();
        }
    }

    private static class CanonicalScalar implements CanonicalValue {
        private final String value;

        private CanonicalScalar(String value) {
            this.value = value;
        }

        @Override
        public void encodeTo(StringBuilder builder) {
            appendEscaped(builder, value);
        }
    }

    private static class CanonicalList implements CanonicalValue {
        private final List<CanonicalValue> values = new ArrayList<>();

        private static CanonicalList list() {
            return new CanonicalList();
        }

        private CanonicalList add(Object value) {
            values.add(canonicalValue(value));
            return this;
        }

        @Override
        public void encodeTo(StringBuilder builder) {
            builder.append('[');
            for (int i = 0; i < values.size(); i++) {
                if (i > 0) {
                    builder.append(',');
                }
                values.get(i).encodeTo(builder);
            }
            builder.append(']');
        }
    }

    private static class CanonicalNode implements CanonicalValue {
        private final String name;
        private final List<CanonicalField> fields = new ArrayList<>();

        private CanonicalNode(String name) {
            this.name = name;
        }

        private static CanonicalNode node(String name) {
            return new CanonicalNode(name);
        }

        private CanonicalNode field(String name, Object value) {
            fields.add(new CanonicalField(name, canonicalValue(value)));
            return this;
        }

        @Override
        public void encodeTo(StringBuilder builder) {
            appendEscaped(builder, name);
            builder.append('[');
            for (int i = 0; i < fields.size(); i++) {
                if (i > 0) {
                    builder.append(',');
                }
                CanonicalField field = fields.get(i);
                appendEscaped(builder, field.name);
                builder.append('=');
                field.value.encodeTo(builder);
            }
            builder.append(']');
        }
    }

    private static class CanonicalField {
        private final String name;
        private final CanonicalValue value;

        private CanonicalField(String name, CanonicalValue value) {
            this.name = name;
            this.value = value;
        }
    }

    private static void appendEscaped(StringBuilder builder, String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\':
                case '[':
                case ']':
                case '{':
                case '}':
                case ',':
                case '=':
                case ':':
                    builder.append('\\').append(c);
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                case '\r':
                    builder.append("\\r");
                    break;
                case '\t':
                    builder.append("\\t");
                    break;
                default:
                    builder.append(c);
                    break;
            }
        }
    }

    private class CanonicalPlanVisitor extends PlanVisitor<CanonicalNode, IvmNormalizeResult> {
        @Override
        public CanonicalNode visit(Plan plan, IvmNormalizeResult normalizeResult) {
            throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                    "IVM layout signature does not support plan node: "
                            + plan.getClass().getSimpleName());
        }

        @Override
        public CanonicalNode visitLogicalResultSink(LogicalResultSink<? extends Plan> sink,
                IvmNormalizeResult normalizeResult) {
            return sink.child().accept(this, normalizeResult);
        }

        @Override
        public CanonicalNode visitLogicalSink(LogicalSink<? extends Plan> sink, IvmNormalizeResult normalizeResult) {
            return sink.child().accept(this, normalizeResult);
        }

        @Override
        public CanonicalNode visitLogicalFilter(LogicalFilter<? extends Plan> filter,
                IvmNormalizeResult normalizeResult) {
            return filter.child().accept(this, normalizeResult);
        }

        @Override
        public CanonicalNode visitLogicalProject(LogicalProject<? extends Plan> project,
                IvmNormalizeResult normalizeResult) {
            return canonicalProject(project, normalizeResult);
        }

        @Override
        public CanonicalNode visitLogicalOlapScan(LogicalOlapScan scan,
                IvmNormalizeResult normalizeResult) {
            return canonicalScan(scan);
        }

        @Override
        public CanonicalNode visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join,
                IvmNormalizeResult normalizeResult) {
            return canonicalJoin(join, normalizeResult);
        }

        @Override
        public CanonicalNode visitLogicalUnion(LogicalUnion union, IvmNormalizeResult normalizeResult) {
            return canonicalUnion(union, normalizeResult);
        }

        @Override
        public CanonicalNode visitLogicalAggregate(LogicalAggregate<? extends Plan> agg,
                IvmNormalizeResult normalizeResult) {
            return canonicalAggregate(agg, normalizeResult);
        }
    }
}
