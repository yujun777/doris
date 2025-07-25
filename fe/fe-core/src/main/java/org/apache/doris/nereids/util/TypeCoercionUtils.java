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

package org.apache.doris.nereids.util;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.BinaryOperator;
import org.apache.doris.nereids.trees.expressions.BitAnd;
import org.apache.doris.nereids.trees.expressions.BitNot;
import org.apache.doris.nereids.trees.expressions.BitOr;
import org.apache.doris.nereids.trees.expressions.BitXor;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IntegralDivide;
import org.apache.doris.nereids.trees.expressions.Mod;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Array;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateMap;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Result;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.expressions.literal.format.DateTimeChecker;
import org.apache.doris.nereids.trees.expressions.literal.format.FloatChecker;
import org.apache.doris.nereids.trees.expressions.literal.format.IntegerChecker;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IPv4Type;
import org.apache.doris.nereids.types.IPv6Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.types.coercion.ComplexDataType;
import org.apache.doris.nereids.types.coercion.FollowToAnyDataType;
import org.apache.doris.nereids.types.coercion.FractionalType;
import org.apache.doris.nereids.types.coercion.IntegralType;
import org.apache.doris.nereids.types.coercion.NumericType;
import org.apache.doris.nereids.types.coercion.PrimitiveType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Utils for type coercion.
 */
public class TypeCoercionUtils {

    /**
     * numeric type precedence for type promotion.
     * bigger numeric has smaller ordinal
     */
    public static final List<DataType> NUMERIC_PRECEDENCE = ImmutableList.of(
            DoubleType.INSTANCE,
            FloatType.INSTANCE,
            LargeIntType.INSTANCE,
            BigIntType.INSTANCE,
            IntegerType.INSTANCE,
            SmallIntType.INSTANCE,
            TinyIntType.INSTANCE
    );

    private static final Logger LOG = LogManager.getLogger(TypeCoercionUtils.class);

    /**
     * ensure the result's data type equals to the originExpr's dataType,
     * ATTN: this method usually used in fold constant rule
     */
    public static Expression ensureSameResultType(
            Expression originExpr, Expression result, ExpressionRewriteContext context) {
        DataType originDataType = originExpr.getDataType();
        DataType newDataType = result.getDataType();
        if (originDataType.equals(newDataType)) {
            return result;
        }
        // backend can direct use all string like type without cast
        if (originDataType.isStringLikeType() && newDataType.isStringLikeType()) {
            return result;
        }
        return FoldConstantRuleOnFE.PATTERN_MATCH_INSTANCE.visitCast(
                new Cast(result, originDataType), context
        );
    }

    /**
     * Return Optional.empty() if we cannot do implicit cast.
     */
    public static Optional<DataType> implicitCast(DataType input, DataType expected) {
        if (input instanceof ArrayType && expected instanceof ArrayType) {
            Optional<DataType> itemType = implicitCast(
                    ((ArrayType) input).getItemType(), ((ArrayType) expected).getItemType());
            return itemType.map(ArrayType::of);
        } else if (input instanceof MapType && expected instanceof MapType) {
            Optional<DataType> keyType = implicitCast(
                    ((MapType) input).getKeyType(), ((MapType) expected).getKeyType());
            Optional<DataType> valueType = implicitCast(
                    ((MapType) input).getValueType(), ((MapType) expected).getValueType());
            if (keyType.isPresent() && valueType.isPresent()) {
                return Optional.of(MapType.of(keyType.get(), valueType.get()));
            }
            return Optional.empty();
        } else if (input instanceof StructType && expected instanceof StructType) {
            List<StructField> inputFields = ((StructType) input).getFields();
            List<StructField> expectedFields = ((StructType) expected).getFields();
            if (inputFields.size() != expectedFields.size()) {
                return Optional.empty();
            }
            List<StructField> newFields = Lists.newArrayList();
            for (int i = 0; i < inputFields.size(); i++) {
                Optional<DataType> newDataType = implicitCast(inputFields.get(i).getDataType(),
                        expectedFields.get(i).getDataType());
                if (newDataType.isPresent()) {
                    newFields.add(inputFields.get(i).withDataType(newDataType.get()));
                } else {
                    return Optional.empty();
                }
            }
            return Optional.of(new StructType(newFields));
        } else if (input instanceof VariantType && (expected.isNumericType() || expected.isStringLikeType())) {
            // variant could implicit cast to numric types and string like types
            return Optional.of(expected);
        } else {
            return implicitCastPrimitive(input, expected);
        }
    }

    /**
     * Return Optional.empty() if we cannot do implicit cast.
     */
    public static Optional<DataType> implicitCastPrimitive(DataType input, DataType expected) {
        Optional<DataType> castType = implicitCastPrimitiveInternal(input, expected);
        // TODO: complete the cast logic like FunctionCallExpr.analyzeImpl
        boolean legacyCastCompatible = false;
        try {
            if (!(expected instanceof AnyDataType) && !(expected instanceof FollowToAnyDataType)) {
                legacyCastCompatible = !input.toCatalogDataType().matchesType(expected.toCatalogDataType());
            }
        } catch (Throwable t) {
            // ignore.
        }
        if (!castType.isPresent() && legacyCastCompatible) {
            castType = Optional.of(expected);
        }
        return castType;
    }

    /**
     * Return Optional.empty() if we cannot do implicit cast.
     */
    @Developing
    private static Optional<DataType> implicitCastPrimitiveInternal(DataType input, DataType expected) {
        DataType returnType = null;
        if (expected.acceptsType(input)) {
            // If the expected type
            // is already a parent of the input type, no need to cast.
            return Optional.of(input);
        }
        if (input instanceof NullType) {
            // Cast null type (usually from null literals) into target types
            returnType = expected.defaultConcreteType();
        } else if (input instanceof NumericType) {
            if (expected instanceof DecimalV2Type) {
                // If input is a numeric type but not decimal, and we expect a decimal type,
                // cast the input to decimal.
                returnType = DecimalV2Type.forType(input);
            } else if (expected instanceof DecimalV3Type) {
                returnType = expected;
            } else if (expected instanceof DateTimeType) {
                returnType = DateTimeType.INSTANCE;
            } else if (expected instanceof NumericType) {
                // For any other numeric types, implicitly cast to each other, e.g. bigint -> int, int -> bigint
                returnType = expected.defaultConcreteType();
            } else if (expected instanceof IPv4Type) {
                returnType = IPv4Type.INSTANCE;
            }
        } else if (input instanceof CharacterType) {
            if (expected instanceof DecimalV2Type) {
                returnType = DecimalV2Type.SYSTEM_DEFAULT;
            } else if (expected instanceof DecimalV3Type) {
                returnType = DecimalV3Type.SYSTEM_DEFAULT;
            } else if (expected instanceof NumericType) {
                returnType = expected.defaultConcreteType();
            } else if (expected instanceof DateTimeType) {
                returnType = DateTimeType.INSTANCE;
            } else if (expected instanceof JsonType) {
                returnType = JsonType.INSTANCE;
            } else if (expected instanceof IPv4Type) {
                returnType = IPv4Type.INSTANCE;
            } else if (expected instanceof IPv6Type) {
                returnType = IPv6Type.INSTANCE;
            }
        } else if (input.isDateType()) {
            if (expected instanceof DateTimeType) {
                returnType = expected.defaultConcreteType();
            }
            if (expected instanceof DateTimeV2Type) {
                returnType = expected;
            }
        } else if (input.isDateTimeV2Type()) {
            if (expected instanceof DateTimeV2Type) {
                returnType = expected;
            }
        }

        if (returnType == null && input instanceof PrimitiveType
                && expected instanceof CharacterType) {
            returnType = expected.defaultConcreteType();
        }

        // could not do implicit cast, just return null. Throw exception in check analysis.
        return Optional.ofNullable(returnType);
    }

    /**
     * return ture if datatype has character type in it, cannot use instance of CharacterType because of complex type.
     */
    public static boolean hasCharacterType(DataType dataType) {
        return hasSpecifiedType(dataType, CharacterType.class);
    }

    public static boolean hasDecimalV2Type(DataType dataType) {
        return hasSpecifiedType(dataType, DecimalV2Type.class);
    }

    public static boolean hasDecimalV3Type(DataType dataType) {
        return hasSpecifiedType(dataType, DecimalV3Type.class);
    }

    public static boolean hasDateTimeV2Type(DataType dataType) {
        return hasSpecifiedType(dataType, DateTimeV2Type.class);
    }

    public static boolean hasTimeV2Type(DataType dataType) {
        return hasSpecifiedType(dataType, TimeV2Type.class);
    }

    private static boolean hasSpecifiedType(DataType dataType, Class<? extends DataType> specifiedType) {
        if (dataType instanceof ArrayType) {
            return hasSpecifiedType(((ArrayType) dataType).getItemType(), specifiedType);
        } else if (dataType instanceof MapType) {
            return hasSpecifiedType(((MapType) dataType).getKeyType(), specifiedType)
                    || hasSpecifiedType(((MapType) dataType).getValueType(), specifiedType);
        } else if (dataType instanceof StructType) {
            return ((StructType) dataType).getFields().stream()
                    .anyMatch(f -> hasSpecifiedType(f.getDataType(), specifiedType));
        }
        return specifiedType.isAssignableFrom(dataType.getClass());
    }

    /**
     * replace all character types to string for correct type coercion
     */
    public static DataType replaceCharacterToString(DataType dataType) {
        return replaceSpecifiedType(dataType, CharacterType.class, StringType.INSTANCE);
    }

    public static DataType replaceDecimalV2WithDefault(DataType dataType) {
        return replaceSpecifiedType(dataType, DecimalV2Type.class, DecimalV2Type.SYSTEM_DEFAULT);
    }

    public static DataType replaceDecimalV3WithTarget(DataType dataType, DecimalV3Type target) {
        return replaceSpecifiedType(dataType, DecimalV3Type.class, target);
    }

    public static DataType replaceDecimalV3WithWildcard(DataType dataType) {
        return replaceSpecifiedType(dataType, DecimalV3Type.class, DecimalV3Type.WILDCARD);
    }

    public static DataType replaceDateTimeV2WithMax(DataType dataType) {
        return replaceSpecifiedType(dataType, DateTimeV2Type.class, DateTimeV2Type.MAX);
    }

    public static DataType replaceTimesWithTargetPrecision(DataType dataType, int targetScale) {
        return replaceSpecifiedType(
                replaceSpecifiedType(dataType, DateTimeV2Type.class, DateTimeV2Type.of(targetScale)), TimeV2Type.class,
                TimeV2Type.of(targetScale));
    }

    /**
     * replace specifiedType in dataType to newType.
     */
    public static DataType replaceSpecifiedType(DataType dataType,
            Class<? extends DataType> specifiedType, DataType newType) {
        if (dataType instanceof ArrayType) {
            return ArrayType.of(replaceSpecifiedType(((ArrayType) dataType).getItemType(), specifiedType, newType),
                    ((ArrayType) dataType).containsNull());
        } else if (dataType instanceof MapType) {
            return MapType.of(replaceSpecifiedType(((MapType) dataType).getKeyType(), specifiedType, newType),
                    replaceSpecifiedType(((MapType) dataType).getValueType(), specifiedType, newType));
        } else if (dataType instanceof StructType) {
            List<StructField> newFields = ((StructType) dataType).getFields().stream()
                    .map(f -> f.withDataType(replaceSpecifiedType(f.getDataType(), specifiedType, newType)))
                    .collect(ImmutableList.toImmutableList());
            return new StructType(newFields);
        } else if (specifiedType.isAssignableFrom(dataType.getClass())) {
            return newType;
        } else {
            return dataType;
        }
    }

    /**
     * The type used for arithmetic operations.
     */
    public static DataType getNumResultType(DataType type) {
        if (type.isNumericType()) {
            return type;
        }
        if (type.isNullType()) {
            return TinyIntType.INSTANCE;
        }
        if (type.isBooleanType()) {
            return TinyIntType.INSTANCE;
        }
        if (type.isDateLikeType()) {
            return BigIntType.INSTANCE;
        }
        if (type.isStringLikeType() || type.isHllType() || type.isTimeType()) {
            return DoubleType.INSTANCE;
        }
        throw new AnalysisException("Cannot cast from " + type + " to numeric type.");
    }

    /**
     * cast input type if input's datatype is not match with dateType.
     */
    public static Expression castIfNotMatchType(Expression input, DataType dataType) {
        if (matchesType(input.getDataType(), dataType)) {
            return input;
        } else {
            return castIfNotSameType(input, dataType);
        }
    }

    private static boolean matchesType(DataType input, DataType target) {
        // TODO use nereids matches type to instead catalog datatype matches type
        if (input instanceof ArrayType && target instanceof ArrayType) {
            return matchesType(((ArrayType) input).getItemType(), ((ArrayType) target).getItemType());
        } else if (input instanceof MapType && target instanceof MapType) {
            return matchesType(((MapType) input).getKeyType(), ((MapType) target).getKeyType())
                    && matchesType(((MapType) input).getValueType(), ((MapType) target).getValueType());
        } else if (input instanceof StructType && target instanceof StructType) {
            List<StructField> inputFields = ((StructType) input).getFields();
            List<StructField> targetFields = ((StructType) target).getFields();
            if (inputFields.size() != targetFields.size()) {
                return false;
            }
            for (int i = 0; i < inputFields.size(); i++) {
                if (!matchesType(inputFields.get(i).getDataType(), targetFields.get(i).getDataType())) {
                    return false;
                }
            }
            return true;
        } else {
            if (input instanceof NullType) {
                return false;
            }
            return input.toCatalogDataType().matchesType(target.toCatalogDataType());
        }
    }

    /**
     * cast input type if input's datatype is not same with dateType.
     */
    public static Expression castIfNotSameType(Expression input, DataType targetType) {
        if (input.isNullLiteral()) {
            return new NullLiteral(targetType);
        } else if (input.getDataType().equals(targetType)
                || (input.getDataType().isStringLikeType()) && targetType.isStringLikeType()) {
            return input;
        } else {
            checkCanCastTo(input.getDataType(), targetType);
            return unSafeCast(input, targetType);
        }
    }

    public static Expression castUnbound(Expression expression, DataType targetType) {
        if (expression instanceof Literal) {
            return TypeCoercionUtils.castIfNotSameType(expression, targetType);
        } else {
            return TypeCoercionUtils.unSafeCast(expression, targetType);
        }
    }

    /**
     * like castIfNotSameType does, but varchar or char type would be cast to target length exactly
     */
    public static Expression castIfNotSameTypeStrict(Expression input, DataType targetType) {
        if (input.isNullLiteral()) {
            return new NullLiteral(targetType);
        } else if (input.getDataType().equals(targetType)) {
            return input;
        } else {
            checkCanCastTo(input.getDataType(), targetType);
            return unSafeCast(input, targetType);
        }
    }

    private static boolean canCastTo(DataType input, DataType target) {
        return Type.canCastTo(input.toCatalogDataType(), target.toCatalogDataType());
    }

    public static void checkCanCastTo(DataType input, DataType target) {
        if (canCastTo(input, target)) {
            return;
        }
        throw new AnalysisException("can not cast from origin type " + input + " to target type=" + target);
    }

    private static Expression unSafeCast(Expression input, DataType dataType) {
        if (input instanceof Literal) {
            DataType type = input.getDataType();
            while (!type.equals(dataType)) {
                DataType promoted = type.promotion();
                if (type.equals(promoted)) {
                    break;
                }
                type = promoted;
            }
            if (type.equals(dataType)) {
                Literal promoted = DataType.promoteLiteral(((Literal) input).getValue(), dataType);
                if (promoted != null) {
                    return promoted;
                }
            }
        }
        return recordTypeCoercionForSubQuery(input, dataType);
    }

    private static Expression recordTypeCoercionForSubQuery(Expression input, DataType dataType) {
        if (input instanceof SubqueryExpr) {
            return ((SubqueryExpr) input).withTypeCoercion(dataType);
        }
        return new Cast(input, dataType);
    }

    /**
     * convert character literal to another side type.
     */
    @Developing
    public static <T extends BinaryOperator> T processCharacterLiteralInBinaryOperator(
            T op, Expression left, Expression right) {
        if (!(left instanceof Literal) && !(right instanceof Literal)) {
            return (T) op.withChildren(left, right);
        }
        if (left instanceof Literal && ((Literal) left).isStringLikeLiteral()
                && !right.getDataType().isStringLikeType()) {
            left = TypeCoercionUtils.characterLiteralTypeCoercion(
                    ((Literal) left).getStringValue(), right.getDataType()).orElse(left);
        }
        if (right instanceof Literal && ((Literal) right).isStringLikeLiteral()
                && !left.getDataType().isStringLikeType()) {
            right = TypeCoercionUtils.characterLiteralTypeCoercion(
                    ((Literal) right).getStringValue(), left.getDataType()).orElse(right);

        }
        return (T) op.withChildren(left, right);
    }

    /**
     * characterLiteralTypeCoercion.
     */
    @Developing
    public static Optional<Expression> characterLiteralTypeCoercion(String value, DataType dataType) {
        Expression ret = null;
        try {
            if (dataType instanceof BooleanType) {
                if ("true".equalsIgnoreCase(value)) {
                    ret = BooleanLiteral.TRUE;
                }
                if ("false".equalsIgnoreCase(value)) {
                    ret = BooleanLiteral.FALSE;
                }
            } else if (dataType instanceof TinyIntType && IntegerChecker.isValidInteger(value)) {
                BigInteger bigInt = new BigInteger(value);
                if (BigInteger.valueOf(bigInt.byteValue()).equals(bigInt)) {
                    ret = new TinyIntLiteral(bigInt.byteValue());
                } else if (BigInteger.valueOf(bigInt.shortValue()).equals(bigInt)) {
                    ret = new SmallIntLiteral(bigInt.shortValue());
                } else if (BigInteger.valueOf(bigInt.intValue()).equals(bigInt)) {
                    ret = new IntegerLiteral(bigInt.intValue());
                } else if (BigInteger.valueOf(bigInt.longValue()).equals(bigInt)) {
                    ret = new BigIntLiteral(bigInt.longValueExact());
                } else {
                    ret = new LargeIntLiteral(bigInt);
                }
            } else if (dataType instanceof SmallIntType && IntegerChecker.isValidInteger(value)) {
                BigInteger bigInt = new BigInteger(value);
                if (BigInteger.valueOf(bigInt.shortValue()).equals(bigInt)) {
                    ret = new SmallIntLiteral(bigInt.shortValue());
                } else if (BigInteger.valueOf(bigInt.intValue()).equals(bigInt)) {
                    ret = new IntegerLiteral(bigInt.intValue());
                } else if (BigInteger.valueOf(bigInt.longValue()).equals(bigInt)) {
                    ret = new BigIntLiteral(bigInt.longValueExact());
                } else {
                    ret = new LargeIntLiteral(bigInt);
                }
            } else if (dataType instanceof IntegerType && IntegerChecker.isValidInteger(value)) {
                BigInteger bigInt = new BigInteger(value);
                if (BigInteger.valueOf(bigInt.intValue()).equals(bigInt)) {
                    ret = new IntegerLiteral(bigInt.intValue());
                } else if (BigInteger.valueOf(bigInt.longValue()).equals(bigInt)) {
                    ret = new BigIntLiteral(bigInt.longValueExact());
                } else {
                    ret = new LargeIntLiteral(bigInt);
                }
            } else if (dataType instanceof BigIntType && IntegerChecker.isValidInteger(value)) {
                BigInteger bigInt = new BigInteger(value);
                if (BigInteger.valueOf(bigInt.longValue()).equals(bigInt)) {
                    ret = new BigIntLiteral(bigInt.longValueExact());
                } else {
                    ret = new LargeIntLiteral(bigInt);
                }
            } else if (dataType instanceof LargeIntType && IntegerChecker.isValidInteger(value)) {
                BigInteger bigInt = new BigInteger(value);
                ret = new LargeIntLiteral(bigInt);
            } else if (dataType instanceof FloatType && FloatChecker.isValidFloat(value)) {
                ret = new FloatLiteral(Float.parseFloat(value));
            } else if (dataType instanceof DoubleType && FloatChecker.isValidFloat(value)) {
                ret = new DoubleLiteral(Double.parseDouble(value));
            } else if (dataType instanceof DecimalV2Type && FloatChecker.isValidFloat(value)) {
                ret = new DecimalLiteral(new BigDecimal(value));
            } else if (dataType instanceof DecimalV3Type && FloatChecker.isValidFloat(value)) {
                ret = new DecimalV3Literal((DecimalV3Type) dataType, new BigDecimal(value));
            } else if (dataType instanceof CharType) {
                ret = new VarcharLiteral(value, ((CharType) dataType).getLen());
            } else if (dataType instanceof VarcharType) {
                ret = new VarcharLiteral(value, ((VarcharType) dataType).getLen());
            } else if (dataType instanceof StringType) {
                ret = new StringLiteral(value);
            } else if (dataType.isDateTimeV2Type() && DateTimeChecker.isValidDateTime(value)) {
                ret = DateTimeLiteral.parseDateTimeLiteral(value, true).orElse(null);
            } else if (dataType.isDateTimeType() && DateTimeChecker.isValidDateTime(value)) {
                ret = DateTimeLiteral.parseDateTimeLiteral(value, false).orElse(null);
            } else if (dataType.isDateV2Type() && DateTimeChecker.isValidDateTime(value)) {
                Result<DateLiteral, AnalysisException> parseResult = DateV2Literal.parseDateLiteral(value);
                if (parseResult.isOk()) {
                    ret = parseResult.get();
                } else {
                    Result<DateTimeLiteral, AnalysisException> parseResult2
                            = DateTimeV2Literal.parseDateTimeLiteral(value, true);
                    if (parseResult2.isOk()) {
                        ret = parseResult2.get();
                    }
                }
            } else if (dataType.isDateType() && DateTimeChecker.isValidDateTime(value)) {
                ret = DateLiteral.parseDateLiteral(value).orElse(null);
            }
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("convert '{}' to type {} failed", value, dataType);
            }
        }
        return Optional.ofNullable(ret);

    }

    public static Expression implicitCastInputTypes(Expression expr, List<DataType> expectedInputTypes) {
        List<Optional<DataType>> inputImplicitCastTypes
                = getInputImplicitCastTypes(expr.children(), expectedInputTypes);
        return castInputs(expr, inputImplicitCastTypes);
    }

    private static List<Optional<DataType>> getInputImplicitCastTypes(
            List<Expression> inputs, List<DataType> expectedTypes) {
        Builder<Optional<DataType>> implicitCastTypes = ImmutableList.builderWithExpectedSize(inputs.size());
        for (int i = 0; i < inputs.size(); i++) {
            DataType argType = inputs.get(i).getDataType();
            DataType expectedType = expectedTypes.get(i);
            Optional<DataType> castType = TypeCoercionUtils.implicitCast(argType, expectedType);
            implicitCastTypes.add(castType);
        }
        return implicitCastTypes.build();
    }

    private static Expression castInputs(Expression expr, List<Optional<DataType>> castTypes) {
        return expr.withChildren((child, childIndex) -> {
            DataType argType = child.getDataType();
            Optional<DataType> castType = castTypes.get(childIndex);
            if (castType.isPresent() && !castType.get().equals(argType)) {
                return TypeCoercionUtils.castIfNotMatchType(child, castType.get());
            } else {
                return child;
            }
        });
    }

    /**
     * process BoundFunction type coercion
     */
    public static Expression processBoundFunction(BoundFunction boundFunction) {
        // check
        boundFunction.checkLegalityBeforeTypeCoercion();

        if (boundFunction instanceof CreateMap) {
            return processCreateMap((CreateMap) boundFunction);
        }

        // type coercion
        return implicitCastInputTypes(boundFunction, boundFunction.expectedInputTypes());
    }

    private static Expression processCreateMap(CreateMap createMap) {
        if (createMap.arity() == 0) {
            return new MapLiteral();
        }
        List<Expression> keys = Lists.newArrayList();
        List<Expression> values = Lists.newArrayList();
        for (int i = 0; i < createMap.arity(); i++) {
            if (i % 2 == 0) {
                keys.add(createMap.child(i));
            } else {
                values.add(createMap.child(i));
            }
        }
        // TODO: use the find common type to get key and value type after we redefine type coercion in Doris.
        Array keyArray = new Array(keys.toArray(new Expression[0]));
        Array valueArray = new Array(values.toArray(new Expression[0]));
        keyArray = (Array) implicitCastInputTypes(keyArray, keyArray.expectedInputTypes());
        valueArray = (Array) implicitCastInputTypes(valueArray, valueArray.expectedInputTypes());
        DataType keyType = ((ArrayType) (keyArray.getDataType())).getItemType();
        DataType valueType = ((ArrayType) (valueArray.getDataType())).getItemType();
        ImmutableList.Builder<Expression> newChildren = ImmutableList.builder();
        for (int i = 0; i < createMap.arity(); i++) {
            if (i % 2 == 0) {
                newChildren.add(castIfNotSameType(createMap.child(i), keyType));
            } else {
                newChildren.add(castIfNotSameType(createMap.child(i), valueType));
            }
        }
        return createMap.withChildren(newChildren.build());
    }

    /**
     * process divide
     */
    public static Expression processDivide(Divide divide) {
        // check
        divide.checkLegalityBeforeTypeCoercion();

        Expression left = divide.left();
        Expression right = divide.right();

        DataType t1 = TypeCoercionUtils.getNumResultType(left.getDataType());
        DataType t2 = TypeCoercionUtils.getNumResultType(right.getDataType());

        if (!(left.getDataType() instanceof NumericType || left.getDataType() instanceof BooleanType)) {
            left = castIfNotSameType(left, t1);
        }
        if (!(right.getDataType() instanceof NumericType || right.getDataType() instanceof BooleanType)) {
            right = castIfNotSameType(right, t2);
        }

        DataType commonType = DoubleType.INSTANCE;
        if (t1.isFloatLikeType() || t2.isFloatLikeType()) {
            // double type
        } else if (t1.isDecimalV3Type() || t2.isDecimalV3Type()
                // decimalv2 vs bigint, largeint treat as decimalv3
                || ((t1.isBigIntType() || t1.isLargeIntType()) && t2.isDecimalV2Type())
                || (t1.isDecimalV2Type() && (t2.isBigIntType() || t2.isLargeIntType()))) {
            // divide should cast to precision and target scale
            DecimalV3Type dt1 = DecimalV3Type.forType(t1);
            DecimalV3Type dt2 = DecimalV3Type.forType(t2);
            DecimalV3Type retType = divide.getDataTypeForDecimalV3(dt1, dt2);
            return divide.withChildren(castIfNotSameType(left, retType), castIfNotSameType(right, dt2));
        } else if (t1.isDecimalV2Type() || t2.isDecimalV2Type()) {
            commonType = DecimalV2Type.SYSTEM_DEFAULT;
        }

        Expression newLeft = TypeCoercionUtils.castIfNotSameType(left, commonType);
        Expression newRight = TypeCoercionUtils.castIfNotSameType(right, commonType);
        return divide.withChildren(newLeft, newRight);
    }

    /**
     * process divide
     */
    public static Expression processIntegralDivide(IntegralDivide divide) {
        // check
        divide.checkLegalityBeforeTypeCoercion();

        Expression left = divide.left();
        Expression right = divide.right();

        DataType t1 = TypeCoercionUtils.getNumResultType(left.getDataType());
        DataType t2 = TypeCoercionUtils.getNumResultType(right.getDataType());
        left = castIfNotSameType(left, t1);
        right = castIfNotSameType(right, t2);

        DataType commonType = BigIntType.INSTANCE;
        if (t1.isIntegralType() && t2.isIntegralType()) {
            for (DataType dataType : TypeCoercionUtils.NUMERIC_PRECEDENCE) {
                if (t1.equals(dataType) || t2.equals(dataType)) {
                    commonType = dataType;
                    break;
                }
            }
        }

        return castChildren(divide, left, right, commonType);
    }

    private static Expression castChildren(Expression parent, Expression left, Expression right, DataType commonType) {
        return parent.withChildren(castIfNotSameType(left, commonType), castIfNotSameType(right, commonType));
    }

    /**
     * process BitNot type coercion, cast child to bigint.
     */
    public static Expression processBitNot(BitNot bitNot) {
        Expression child = bitNot.child();
        if (!(child.getDataType().isIntegralType() || child.getDataType().isBooleanType())) {
            child = new Cast(child, BigIntType.INSTANCE);
        }
        if (child != bitNot.child()) {
            return bitNot.withChildren(child);
        } else {
            return bitNot;
        }
    }

    /**
     * binary arithmetic type coercion
     */
    public static Expression processBinaryArithmetic(BinaryArithmetic binaryArithmetic) {
        // check
        binaryArithmetic.checkLegalityBeforeTypeCoercion();

        Expression left = binaryArithmetic.left();
        Expression right = binaryArithmetic.right();

        // characterLiteralTypeCoercion
        // we do this because string is cast to double by default
        // but if string literal could be cast to small type, we could use smaller type than double.
        binaryArithmetic = TypeCoercionUtils.processCharacterLiteralInBinaryOperator(binaryArithmetic, left, right);

        // check string literal can cast to double
        for (Expression expr : binaryArithmetic.children()) {
            if (expr instanceof StringLikeLiteral) {
                try {
                    new BigDecimal(((StringLikeLiteral) expr).getStringValue());
                } catch (NumberFormatException e) {
                    throw new IllegalStateException(String.format(
                            "string literal %s cannot be cast to double", expr.toSql()));
                }
            }
        }

        // 1. choose default numeric type for left and right
        DataType t1 = TypeCoercionUtils.getNumResultType(left.getDataType());
        DataType t2 = TypeCoercionUtils.getNumResultType(right.getDataType());

        if (!(left.getDataType() instanceof NumericType || left.getDataType() instanceof BooleanType)) {
            left = castIfNotSameType(left, t1);
        }
        if (!(right.getDataType() instanceof NumericType || right.getDataType() instanceof BooleanType)) {
            right = castIfNotSameType(right, t2);
        }

        // 2. find common type for left and right
        DataType commonType = t1;
        for (DataType dataType : NUMERIC_PRECEDENCE) {
            if (t1.equals(dataType) || t2.equals(dataType)) {
                commonType = dataType;
                break;
            }
        }
        if (commonType.isFloatLikeType() && (t1.isDecimalV3Type() || t2.isDecimalV3Type())) {
            commonType = DoubleType.INSTANCE;
        }

        if (t1.isDecimalV2Type() || t2.isDecimalV2Type()) {
            // to be consistent with old planner
            // see findCommonType() method in ArithmeticExpr.java
            commonType = DecimalV2Type.SYSTEM_DEFAULT;
        }

        boolean isBitArithmetic = binaryArithmetic instanceof BitAnd
                || binaryArithmetic instanceof BitOr
                || binaryArithmetic instanceof BitXor;

        boolean isArithmetic = binaryArithmetic instanceof Add
                || binaryArithmetic instanceof Subtract
                || binaryArithmetic instanceof Multiply
                || binaryArithmetic instanceof Mod;

        if (isBitArithmetic || binaryArithmetic instanceof IntegralDivide) {
            // float, double and decimal should cast to bigint
            if (t1 instanceof FractionalType || t2 instanceof FractionalType) {
                commonType = BigIntType.INSTANCE;
            }
            return castChildren(binaryArithmetic, left, right, commonType);
        }

        // mod retain float % float
        if (binaryArithmetic instanceof Mod && t1.isFloatType() && t2.isFloatType()) {
            return binaryArithmetic;
        }

        // we treat decimalv2 vs dicimalv3, largeint or bigint as decimalv3 way.
        if ((t1.isDecimalV3Type() || t1.isBigIntType() || t1.isLargeIntType()) && t2.isDecimalV2Type()
                || t1.isDecimalV2Type() && (t2.isDecimalV3Type() || t2.isBigIntType() || t2.isLargeIntType())) {
            return processDecimalV3BinaryArithmetic(binaryArithmetic, left, right);
        }

        // if double as common type, all arithmetic should cast both side to double
        if (isArithmetic && commonType.isDoubleType()) {
            return castChildren(binaryArithmetic, left, right, commonType);
        }

        // double and float already process, we only process decimalv3 and fixed point number.
        if (t1 instanceof DecimalV3Type || t2 instanceof DecimalV3Type) {
            return processDecimalV3BinaryArithmetic(binaryArithmetic, left, right);
        }

        // double, float and decimalv3 already process, we only process fixed point number
        if (t1 instanceof DecimalV2Type || t2 instanceof DecimalV2Type) {
            return castChildren(binaryArithmetic, left, right, DecimalV2Type.SYSTEM_DEFAULT);
        }

        // both side are fixed point number
        if (binaryArithmetic instanceof Divide) {
            return castChildren(binaryArithmetic, left, right, DoubleType.INSTANCE);
        }

        // add, subtract and multiply do not need to cast children for fixed point type
        return castChildren(binaryArithmetic, left, right, commonType.promotion());
    }

    /**
     * process timestamp arithmetic type coercion.
     */
    public static Expression processTimestampArithmetic(TimestampArithmetic timestampArithmetic) {
        // check
        timestampArithmetic.checkLegalityBeforeTypeCoercion();

        Expression left = timestampArithmetic.left();
        Expression right = timestampArithmetic.right();
        // left
        DataType leftType = left.getDataType();

        if (!leftType.isDateLikeType()) {
            if (Config.enable_date_conversion && canCastTo(leftType, DateTimeV2Type.SYSTEM_DEFAULT)) {
                leftType = DateTimeV2Type.SYSTEM_DEFAULT;
            } else if (canCastTo(leftType, DateTimeType.INSTANCE)) {
                leftType = DateTimeType.INSTANCE;
            } else {
                throw new AnalysisException("Operand '" + left.toSql()
                        + "' of timestamp arithmetic expression '" + timestampArithmetic.toSql() + "' returns type '"
                        + left.getDataType() + "'. Expected type 'TIMESTAMP/DATE/DATETIME'.");
            }
        }
        if (leftType.isDateType() && timestampArithmetic.getTimeUnit().isDateTimeUnit()) {
            leftType = DateTimeType.INSTANCE;
        }
        if (leftType.isDateV2Type() && timestampArithmetic.getTimeUnit().isDateTimeUnit()) {
            leftType = DateTimeV2Type.SYSTEM_DEFAULT;
        }
        if (!left.getDataType().isDateLikeType() && !left.getDataType().isNullType()) {
            checkCanCastTo(left.getDataType(), leftType);
            left = castIfNotSameType(left, leftType);
        }

        // right
        if (!(right.getDataType() instanceof PrimitiveType)) {
            throw new AnalysisException("the second argument must be a scalar type. but it is " + right.toSql());
        }
        if (!right.getDataType().isIntegerType()) {
            if (!ScalarType.canCastTo((ScalarType) right.getDataType().toCatalogDataType(), Type.INT)) {
                throw new AnalysisException("Operand '" + right.toSql()
                        + "' of timestamp arithmetic expression '" + timestampArithmetic.toSql() + "' returns type '"
                        + right.getDataType() + "' which is incompatible with expected type 'INT'.");
            }
            right = castIfNotSameType(right, IntegerType.INSTANCE);
        }

        return timestampArithmetic.withChildren(left, right);
    }

    /**
     * process comparison predicate type coercion.
     */
    public static Expression processComparisonPredicate(ComparisonPredicate comparisonPredicate) {
        // check
        comparisonPredicate.checkLegalityBeforeTypeCoercion();

        Expression left = comparisonPredicate.left();
        Expression right = comparisonPredicate.right();

        // same type
        if (left.getDataType().equals(right.getDataType())) {
            if (!supportCompare(left.getDataType())) {
                throw new AnalysisException("data type " + left.getDataType()
                        + " could not used in ComparisonPredicate " + comparisonPredicate.toSql());
            }
            return comparisonPredicate;
        }

        // process string literal with numeric
        comparisonPredicate = TypeCoercionUtils
                .processCharacterLiteralInBinaryOperator(comparisonPredicate, left, right);
        left = comparisonPredicate.left();
        right = comparisonPredicate.right();

        Optional<DataType> commonType = findWiderTypeForTwoForComparison(
                left.getDataType(), right.getDataType(), false);

        if (commonType.isPresent()) {
            commonType = Optional.of(downgradeDecimalAndDateLikeType(
                    commonType.get(),
                    left,
                    right));
            commonType = Optional.of(downgradeDecimalAndDateLikeType(
                    commonType.get(),
                    right,
                    left));
        }

        if (commonType.isPresent()) {
            if (!supportCompare(commonType.get())) {
                throw new AnalysisException("data type " + commonType.get()
                        + " could not used in ComparisonPredicate " + comparisonPredicate.toSql());
            }
            left = castIfNotSameType(left, commonType.get());
            right = castIfNotSameType(right, commonType.get());
        }
        return comparisonPredicate.withChildren(left, right);
    }

    /**
     * process in predicate type coercion.
     */
    public static Expression processInPredicate(InPredicate inPredicate) {
        // check
        inPredicate.checkLegalityBeforeTypeCoercion();

        if (inPredicate.getOptions().stream().map(Expression::getDataType)
                .allMatch(dt -> dt.equals(inPredicate.getCompareExpr().getDataType()))) {
            if (!supportCompare(inPredicate.getCompareExpr().getDataType())
                    && !inPredicate.getCompareExpr().getDataType().isStructType() && !inPredicate.getCompareExpr()
                    .getDataType().isArrayType()) {
                throw new AnalysisException("data type " + inPredicate.getCompareExpr().getDataType()
                        + " could not used in InPredicate " + inPredicate.toSql());
            }
            return inPredicate;
        }
        // process string literal with numeric
        boolean hitString = false;
        List<Expression> newOptions = new ArrayList<>(inPredicate.getOptions());
        if (!(inPredicate.getCompareExpr().getDataType().isStringLikeType())) {
            ListIterator<Expression> iterator = newOptions.listIterator();
            while (iterator.hasNext()) {
                Expression origOption = iterator.next();
                if (origOption instanceof Literal && ((Literal) origOption).isStringLikeLiteral()) {
                    Optional<Expression> option = TypeCoercionUtils.characterLiteralTypeCoercion(
                            ((Literal) origOption).getStringValue(), inPredicate.getCompareExpr().getDataType());
                    if (option.isPresent()) {
                        iterator.set(option.get());
                        hitString = true;
                    }
                }
            }
        }
        final InPredicate fmtInPredicate =
                hitString ? new InPredicate(inPredicate.getCompareExpr(), newOptions) : inPredicate;

        Optional<DataType> optionalCommonType = TypeCoercionUtils.findWiderCommonTypeForComparison(
                fmtInPredicate.children()
                        .stream()
                        .map(Expression::getDataType).collect(Collectors.toList()),
                true);
        if (inPredicate.getCompareExpr().getDataType().isStructType() && optionalCommonType.isPresent()
                && !optionalCommonType.get().isStructType()) {
            throw new AnalysisException("data type " + optionalCommonType.get()
                    + " is not match " + inPredicate.getCompareExpr().getDataType() + " used in InPredicate");
        }
        if (inPredicate.getCompareExpr().getDataType().isArrayType() && optionalCommonType.isPresent()
                && !optionalCommonType.get().isArrayType()) {
            throw new AnalysisException("data type " + optionalCommonType.get()
                    + " is not match " + inPredicate.getCompareExpr().getDataType() + " used in InPredicate");
        }
        if (optionalCommonType.isPresent() && !supportCompare(optionalCommonType.get())
                && !optionalCommonType.get().isStructType() && !optionalCommonType.get().isArrayType()) {
            throw new AnalysisException("data type " + optionalCommonType.get()
                    + " could not used in InPredicate " + inPredicate.toSql());
        }

        if (optionalCommonType.isPresent()) {
            optionalCommonType = Optional.of(downgradeDecimalAndDateLikeType(
                    optionalCommonType.get(),
                    fmtInPredicate.getCompareExpr(),
                    fmtInPredicate.getOptions().toArray(new Expression[0])));
        }

        return optionalCommonType
                .map(commonType -> {
                    List<Expression> newChildren = fmtInPredicate.children().stream()
                            .map(e -> TypeCoercionUtils.castIfNotSameType(e, commonType))
                            .collect(Collectors.toList());
                    return fmtInPredicate.withChildren(newChildren);
                })
                .orElse(fmtInPredicate);
    }

    /**
     * if the expression like slot vs literal, then we prefer cast literal to slot type to ensure delete task could run.
     * currently process decimalv2 vs decimalv3, datev1 vs datev2, datetimev1 vs datetimev2.
     */
    private static DataType downgradeDecimalAndDateLikeType(
            DataType commonType,
            Expression target,
            Expression... compareExpressions) {
        if (shouldDowngrade(DecimalV3Type.class, DecimalV2Type.class,
                commonType, target,
                d -> ((DecimalV3Type) d).getRange() <= DecimalV2Type.MAX_PRECISION - DecimalV2Type.MAX_SCALE
                        && ((DecimalV3Type) d).getScale() <= DecimalV2Type.MAX_SCALE,
                o -> o.isLiteral()
                        && (o.getDataType().isDecimalV2Type() || o.getDataType().isDecimalV3Type()),
                compareExpressions)) {
            DecimalV3Type decimalV3Type = (DecimalV3Type) commonType;
            return DecimalV2Type.createDecimalV2Type(decimalV3Type.getPrecision(), decimalV3Type.getScale());
        }
        // cast to datev1 for datev1 slot in (datev1 or datev2 literal)
        if (shouldDowngrade(DateV2Type.class, DateType.class,
                commonType, target,
                d -> true,
                o -> o.isLiteral()
                        && (o.getDataType().isDateType() || o.getDataType().isDateV2Type()),
                compareExpressions)) {
            return DateType.INSTANCE;
        }
        // cast to datetimev1 for datetimev1 slot in (date like literal) if scale is 0
        if (shouldDowngrade(DateTimeV2Type.class, DateTimeType.class,
                commonType, target,
                d -> ((DateTimeV2Type) d).getScale() == 0,
                o -> o.isLiteral() && o.getDataType().isDateLikeType(),
                compareExpressions)) {
            return DateTimeType.INSTANCE;
        }
        return commonType;
    }

    /**
     * check should downgrade from commonTypeClazz to targetTypeClazz.
     *
     * @param commonTypeClazz before downgrade type
     * @param targetTypeClazz try to downgrade to type
     * @param commonType original common type
     * @param target target expression aka slot
     * @param commonTypePredicate constraint for original type
     * @param otherPredicate constraint for other expressions aka literals
     * @param others literals
     * @return true for should downgrade
     */
    private static boolean shouldDowngrade(
            Class<? extends DataType> commonTypeClazz,
            Class<? extends DataType> targetTypeClazz,
            DataType commonType,
            Expression target,
            Function<DataType, Boolean> commonTypePredicate,
            Function<Expression, Boolean> otherPredicate,
            Expression... others) {
        if (!commonTypeClazz.isInstance(commonType)) {
            return false;
        }
        if (!targetTypeClazz.isInstance(target.getDataType())) {
            return false;
        }
        if (!commonTypePredicate.apply(commonType)) {
            return false;
        }
        for (Expression other : others) {
            if (!otherPredicate.apply(other)) {
                return false;
            }
        }
        return true;
    }

    /**
     * process case when type coercion.
     */
    public static Expression processCaseWhen(CaseWhen caseWhen) {
        // check
        caseWhen.checkLegalityBeforeTypeCoercion();

        // type coercion
        List<DataType> dataTypesForCoercion = caseWhen.dataTypesForCoercion();
        if (dataTypesForCoercion.size() <= 1) {
            return caseWhen;
        }
        DataType first = dataTypesForCoercion.get(0);
        if (dataTypesForCoercion.stream().allMatch(dataType -> dataType.equals(first))) {
            return caseWhen;
        }
        Optional<DataType> optionalCommonType = TypeCoercionUtils.findWiderCommonTypeForCaseWhen(dataTypesForCoercion);
        return optionalCommonType
                .map(commonType -> {
                    DataType realCommonType = commonType instanceof DecimalV2Type
                            ? DecimalV3Type.forType(commonType) : commonType;
                    List<Expression> newChildren
                            = caseWhen.getWhenClauses().stream()
                            .map(wc -> {
                                Expression valueExpr = TypeCoercionUtils.castIfNotSameType(
                                        wc.getResult(), realCommonType);
                                // we must cast every child to the common type, and then
                                // FoldConstantRuleOnFe can eliminate some branches and direct
                                // return a branch value
                                if (!valueExpr.getDataType().equals(realCommonType)) {
                                    valueExpr = new Cast(valueExpr, realCommonType);
                                }
                                return wc.withChildren(wc.getOperand(), valueExpr);
                            })
                            .collect(Collectors.toList());
                    caseWhen.getDefaultValue()
                            .map(dv -> {
                                Expression defaultExpr = TypeCoercionUtils.castIfNotSameType(dv, realCommonType);
                                if (!defaultExpr.getDataType().equals(realCommonType)) {
                                    defaultExpr = new Cast(defaultExpr, realCommonType);
                                }
                                return defaultExpr;
                            })
                            .ifPresent(newChildren::add);
                    return caseWhen.withChildren(newChildren);
                })
                .orElseThrow(() -> new AnalysisException("Cannot find common type for case when " + caseWhen));
    }

    private static boolean canCompareDate(DataType t1, DataType t2) {
        DataType dateType = t1;
        DataType anotherType = t2;
        if (t2.isDateLikeType()) {
            dateType = t2;
            anotherType = t1;
        }
        if (dateType.isDateLikeType() && (anotherType.isDateLikeType() || anotherType.isStringLikeType()
                || anotherType.isHllType() || anotherType.isIntegerLikeType())) {
            return true;
        }
        return false;
    }

    private static boolean maybeCastToVarchar(DataType t) {
        return t.isVarcharType() || t.isCharType() || t.isTimeType() || t.isJsonType() || t.isHllType()
                || t.isBitmapType() || t.isQuantileStateType() || t.isAggStateType();
    }

    public static Optional<DataType> findWiderCommonTypeForComparison(List<DataType> dataTypes) {
        return findWiderCommonTypeForComparison(dataTypes, false);
    }

    @Developing
    private static Optional<DataType> findWiderCommonTypeForComparison(
            List<DataType> dataTypes, boolean intStringToString) {
        Map<Boolean, List<DataType>> partitioned = dataTypes.stream()
                .collect(Collectors.partitioningBy(TypeCoercionUtils::hasCharacterType));
        List<DataType> needTypeCoercion = Lists.newArrayList(Sets.newHashSet(partitioned.get(true)));
        if (needTypeCoercion.size() > 1 || !partitioned.get(false).isEmpty()) {
            needTypeCoercion = needTypeCoercion.stream()
                    .map(TypeCoercionUtils::replaceCharacterToString)
                    .collect(Collectors.toList());
        }
        needTypeCoercion.addAll(partitioned.get(false));
        return needTypeCoercion.stream().map(Optional::of).reduce(Optional.of(NullType.INSTANCE),
                (r, c) -> {
                    if (r.isPresent() && c.isPresent()) {
                        return findWiderTypeForTwoForComparison(r.get(), c.get(), intStringToString);
                    } else {
                        return Optional.empty();
                    }
                });
    }

    /**
     * find wider common type for two data type.
     */
    @Developing
    private static Optional<DataType> findWiderTypeForTwoForComparison(
            DataType left, DataType right, boolean intStringToString) {
        // TODO: need to rethink how to handle char and varchar to return char or varchar as much as possible.
        if (left instanceof ComplexDataType) {
            Optional<DataType> commonType = findCommonComplexTypeForComparison(left, right, intStringToString);
            if (commonType.isPresent()) {
                return commonType;
            }
        }
        return findCommonPrimitiveTypeForComparison(left, right, intStringToString);
    }

    /**
     * find common type for complex type.
     */
    @Developing
    private static Optional<DataType> findCommonComplexTypeForComparison(
            DataType left, DataType right, boolean intStringToString) {
        if (left instanceof ArrayType && right instanceof ArrayType) {
            Optional<DataType> itemType = findWiderTypeForTwoForComparison(
                    ((ArrayType) left).getItemType(), ((ArrayType) right).getItemType(), intStringToString);
            return itemType.map(ArrayType::of);
        } else if (left instanceof MapType && right instanceof MapType) {
            Optional<DataType> keyType = findWiderTypeForTwoForComparison(
                    ((MapType) left).getKeyType(), ((MapType) right).getKeyType(), intStringToString);
            Optional<DataType> valueType = findWiderTypeForTwoForComparison(
                    ((MapType) left).getValueType(), ((MapType) right).getValueType(), intStringToString);
            if (keyType.isPresent() && valueType.isPresent()) {
                return Optional.of(MapType.of(keyType.get(), valueType.get()));
            }
        } else if (left instanceof StructType && right instanceof StructType) {
            List<StructField> leftFields = ((StructType) left).getFields();
            List<StructField> rightFields = ((StructType) right).getFields();
            if (leftFields.size() != rightFields.size()) {
                return Optional.empty();
            }
            List<StructField> newFields = Lists.newArrayList();
            for (int i = 0; i < leftFields.size(); i++) {
                Optional<DataType> newDataType = findWiderTypeForTwoForComparison(leftFields.get(i).getDataType(),
                        rightFields.get(i).getDataType(), intStringToString);
                if (newDataType.isPresent()) {
                    newFields.add(leftFields.get(i).withDataType(newDataType.get()));
                } else {
                    return Optional.empty();
                }
            }
            return Optional.of(new StructType(newFields));
        }
        return Optional.empty();
    }

    /**
     * get common type for comparison.
     * in legacy planner, comparison predicate convert int vs string to double.
     * however, in predicate and between predicate convert int vs string to string
     * but after between rewritten to comparison predicate,
     * int vs string been convert to double again
     * so, in Nereids, only in predicate set this flag to true.
     */
    private static Optional<DataType> findCommonPrimitiveTypeForComparison(
            DataType leftType, DataType rightType, boolean intStringToString) {
        // same type
        if (leftType.equals(rightType)) {
            return Optional.of(leftType);
        }

        if (leftType.isNullType()) {
            return Optional.of(rightType);
        }
        if (rightType.isNullType()) {
            return Optional.of(leftType);
        }

        // decimal v3
        if (leftType.isDecimalV3Type() && rightType.isDecimalV3Type()) {
            return Optional.of(DecimalV3Type.widerDecimalV3Type(
                    (DecimalV3Type) leftType, (DecimalV3Type) rightType, true));
        }

        // decimal v2
        if (leftType.isDecimalV2Type() && rightType.isDecimalV2Type()) {
            return Optional.of(DecimalV2Type.widerDecimalV2Type((DecimalV2Type) leftType, (DecimalV2Type) rightType));
        }

        // date
        if (canCompareDate(leftType, rightType)) {
            if (leftType.isDateTimeV2Type() && rightType.isDateTimeV2Type()) {
                return Optional.of(DateTimeV2Type.getWiderDatetimeV2Type(
                        (DateTimeV2Type) leftType, (DateTimeV2Type) rightType));
            } else if (leftType.isDateTimeV2Type()) {
                if (rightType instanceof IntegralType || rightType.isDateType()
                        || rightType.isDateV2Type() || rightType.isDateTimeType()) {
                    return Optional.of(leftType);
                } else {
                    return Optional.of(DateTimeV2Type.MAX);
                }
            } else if (rightType.isDateTimeV2Type()) {
                if (leftType instanceof IntegralType || leftType.isDateType()
                        || leftType.isDateV2Type() || leftType.isDateTimeType()) {
                    return Optional.of(rightType);
                } else {
                    return Optional.of(DateTimeV2Type.MAX);
                }
            } else if (leftType.isDateV2Type()) {
                if (rightType instanceof IntegralType || rightType.isDateType() || rightType.isDateV2Type()) {
                    return Optional.of(leftType);
                } else if (rightType.isDateTimeType() || rightType.isStringLikeType() || rightType.isHllType()) {
                    return Optional.of(DateTimeV2Type.SYSTEM_DEFAULT);
                } else {
                    return Optional.of(DateTimeV2Type.MAX);
                }
            } else if (rightType.isDateV2Type()) {
                if (leftType instanceof IntegralType || leftType.isDateType() || leftType.isDateV2Type()) {
                    return Optional.of(rightType);
                } else if (leftType.isDateTimeType() || leftType.isStringLikeType() || leftType.isHllType()) {
                    return Optional.of(DateTimeV2Type.SYSTEM_DEFAULT);
                } else {
                    return Optional.of(DateTimeV2Type.MAX);
                }
            } else {
                return Optional.of(DateTimeType.INSTANCE);
            }
        }

        // varchar-like vs varchar-like
        if (maybeCastToVarchar(leftType) && maybeCastToVarchar(rightType)) {
            return Optional.of(VarcharType.SYSTEM_DEFAULT);
        }

        // varchar-like vs string
        if ((maybeCastToVarchar(leftType) && rightType instanceof StringType)
                || (maybeCastToVarchar(rightType) && leftType instanceof StringType)) {
            return Optional.of(StringType.INSTANCE);
        }

        // in legacy planner, comparison predicate convert int vs string to double.
        // however, in predicate and between predicate convert int vs string to string
        // but after between rewritten to comparison predicate,
        // int vs string been convert to double again
        // so, in Nereids, only in predicate set this flag to true.
        if (intStringToString) {
            if ((leftType instanceof IntegralType && rightType.isStringLikeType())
                    || (rightType instanceof IntegralType && leftType.isStringLikeType())) {
                return Optional.of(StringType.INSTANCE);
            }
        }

        // numeric
        if (leftType.isFloatType() || leftType.isDoubleType()
                || rightType.isFloatType() || rightType.isDoubleType()) {
            return Optional.of(DoubleType.INSTANCE);
        }
        if (leftType.isNumericType() && rightType.isNumericType()) {
            DataType commonType = leftType;
            for (DataType dataType : NUMERIC_PRECEDENCE) {
                if (leftType.equals(dataType) || rightType.equals(dataType)) {
                    commonType = dataType;
                    break;
                }
            }
            if (leftType instanceof DecimalV3Type || rightType instanceof DecimalV3Type) {
                return Optional.of(DecimalV3Type.widerDecimalV3Type(
                        DecimalV3Type.forType(leftType), DecimalV3Type.forType(rightType), true));
            }
            if (leftType instanceof DecimalV2Type || rightType instanceof DecimalV2Type) {
                if (leftType instanceof BigIntType || rightType instanceof BigIntType
                        || leftType instanceof LargeIntType || rightType instanceof LargeIntType) {
                    // only decimalv3 can hold big or large int
                    return Optional
                            .of(DecimalV3Type.widerDecimalV3Type(DecimalV3Type.forType(leftType),
                                    DecimalV3Type.forType(rightType), true));
                } else {
                    return Optional.of(DecimalV2Type.widerDecimalV2Type(
                            DecimalV2Type.forType(leftType), DecimalV2Type.forType(rightType)));
                }
            }
            return Optional.of(commonType);
        }

        // ip type
        if ((leftType.isIPv4Type() && rightType.isStringLikeType())
                || (rightType.isIPv4Type() && leftType.isStringLikeType())) {
            return Optional.of(IPv4Type.INSTANCE);
        }
        if ((leftType.isIPv6Type() && rightType.isStringLikeType())
                || (rightType.isIPv6Type() && leftType.isStringLikeType())
                || (leftType.isIPv4Type() && rightType.isIPv6Type())
                || (leftType.isIPv6Type() && rightType.isIPv4Type())) {
            return Optional.of(IPv6Type.INSTANCE);
        }

        // variant type
        if ((leftType.isVariantType() && (rightType.isStringLikeType() || rightType.isNumericType()))) {
            if (rightType.isDecimalLikeType()) {
                // TODO support decimal
                return Optional.of(DoubleType.INSTANCE);
            }
            return Optional.of(rightType);
        }
        if ((rightType.isVariantType() && (leftType.isStringLikeType() || leftType.isNumericType()))) {
            if (leftType.isDecimalLikeType()) {
                // TODO support decimal
                return Optional.of(DoubleType.INSTANCE);
            }
            return Optional.of(leftType);
        }
        return Optional.of(DoubleType.INSTANCE);
    }

    /**
     * find wider common type for data type list.
     */
    public static Optional<DataType> findWiderCommonTypeForCaseWhen(List<DataType> dataTypes) {
        Map<Boolean, List<DataType>> partitioned = dataTypes.stream()
                .collect(Collectors.partitioningBy(TypeCoercionUtils::hasCharacterType));
        List<DataType> needTypeCoercion = Lists.newArrayList(Sets.newHashSet(partitioned.get(true)));
        List<DataType> nonCharTypes = partitioned.get(false);
        if (needTypeCoercion.size() > 1 || !nonCharTypes.isEmpty()) {
            needTypeCoercion = Utils.fastMapList(
                    needTypeCoercion, nonCharTypes.size(), TypeCoercionUtils::replaceCharacterToString);
        }
        needTypeCoercion.addAll(nonCharTypes);

        DataType commonType = NullType.INSTANCE;
        for (DataType dataType : needTypeCoercion) {
            Optional<DataType> newCommonType = findWiderTypeForTwoForCaseWhen(commonType, dataType);
            if (!newCommonType.isPresent()) {
                return Optional.empty();
            }
            commonType = newCommonType.get();
        }
        return Optional.of(commonType);
    }

    /**
     * find wider common type for two data type.
     */
    @Developing
    private static Optional<DataType> findWiderTypeForTwoForCaseWhen(DataType left, DataType right) {
        // TODO: need to rethink how to handle char and varchar to return char or varchar as much as possible.
        Optional<DataType> commonType = findCommonComplexTypeForCaseWhen(left, right);
        if (commonType.isPresent()) {
            return commonType;
        }
        return findCommonPrimitiveTypeForCaseWhen(left, right);
    }

    /**
     * find common type for complex type.
     */
    @Developing
    private static Optional<DataType> findCommonComplexTypeForCaseWhen(DataType left, DataType right) {
        if (left.isNullType()) {
            return Optional.of(right);
        }
        if (right.isNullType()) {
            return Optional.of(left);
        }
        if (left.equals(right)) {
            return Optional.of(left);
        }
        if (left instanceof ArrayType && right instanceof ArrayType) {
            Optional<DataType> itemType = findWiderTypeForTwoForCaseWhen(
                    ((ArrayType) left).getItemType(), ((ArrayType) right).getItemType());
            return itemType.map(ArrayType::of);
        } else if (left instanceof MapType && right instanceof MapType) {
            Optional<DataType> keyType = findWiderTypeForTwoForCaseWhen(
                    ((MapType) left).getKeyType(), ((MapType) right).getKeyType());
            Optional<DataType> valueType = findWiderTypeForTwoForCaseWhen(
                    ((MapType) left).getValueType(), ((MapType) right).getValueType());
            if (keyType.isPresent() && valueType.isPresent()) {
                return Optional.of(MapType.of(keyType.get(), valueType.get()));
            }
        } else if (left instanceof StructType && right instanceof StructType) {
            List<StructField> leftFields = ((StructType) left).getFields();
            List<StructField> rightFields = ((StructType) right).getFields();
            if (leftFields.size() != rightFields.size()) {
                return Optional.empty();
            }
            List<StructField> newFields = Lists.newArrayList();
            for (int i = 0; i < leftFields.size(); i++) {
                Optional<DataType> newDataType = findWiderTypeForTwoForCaseWhen(leftFields.get(i).getDataType(),
                        rightFields.get(i).getDataType());
                if (newDataType.isPresent()) {
                    newFields.add(leftFields.get(i).withDataType(newDataType.get()));
                } else {
                    return Optional.empty();
                }
            }
            return Optional.of(new StructType(newFields));
        }
        return Optional.empty();
    }

    /**
     * two types' common type, see TypeCoercionUtilsTest#testFindCommonPrimitiveTypeForCaseWhen()
     */
    @VisibleForTesting
    protected static Optional<DataType> findCommonPrimitiveTypeForCaseWhen(DataType t1, DataType t2) {
        if (!(t1 instanceof PrimitiveType) || !(t2 instanceof PrimitiveType)) {
            return Optional.empty();
        }

        if (t1.equals(t2)) {
            return Optional.of(t1);
        }

        if (t1.isNullType()) {
            return Optional.of(t2);
        }
        if (t2.isNullType()) {
            return Optional.of(t1);
        }

        // objectType only support compare with itself, so return empty here.
        if (t1.isObjectType() || t2.isObjectType()) {
            return Optional.empty();
        }

        // TODO: support ALL type

        // string-like vs all other type
        if (t1.isStringLikeType() || t2.isStringLikeType()) {
            if ((t1.isCharType() || t1.isVarcharType()) && (t2.isCharType() || t2.isVarcharType())) {
                int len = Math.max(((CharacterType) t1).getLen(), ((CharacterType) t2).getLen());
                if (((CharacterType) t1).getLen() < 0 || ((CharacterType) t2).getLen() < 0) {
                    len = VarcharType.SYSTEM_DEFAULT.getLen();
                }
                return Optional.of(VarcharType.createVarcharType(len));
            }
            return Optional.of(StringType.INSTANCE);
        }

        // forbidden decimal with date
        if ((t1.isDecimalV2Type() && t2.isDateType()) || (t2.isDecimalV2Type() && t1.isDateType())) {
            return Optional.empty();
        }
        if ((t1.isDecimalV2Type() && t2.isDateV2Type()) || (t2.isDecimalV2Type() && t1.isDateV2Type())) {
            return Optional.empty();
        }
        if ((t1.isDecimalV3Type() && t2.isDateType()) || (t2.isDecimalV3Type() && t1.isDateType())) {
            return Optional.empty();
        }
        if ((t1.isDecimalV3Type() && t2.isDateV2Type()) || (t2.isDecimalV3Type() && t1.isDateV2Type())) {
            return Optional.empty();
        }

        // decimalv3 and floating type
        if (t1.isDecimalV3Type() || t2.isDecimalV3Type()) {
            if (t1.isFloatType() || t2.isDoubleType() || t1.isDoubleType() || t2.isFloatType()) {
                return Optional.of(DoubleType.INSTANCE);
            }
        }

        // decimal precision derive
        if (t1.isDecimalV3Type() || t2.isDecimalV3Type()) {
            return Optional.of(DecimalV3Type.widerDecimalV3Type(
                    DecimalV3Type.forType(t1), DecimalV3Type.forType(t2), true));
        }

        // decimalv2 and floating type
        if (t1.isDecimalV2Type() || t2.isDecimalV2Type()) {
            if (t1.isFloatType() || t2.isDoubleType() || t1.isDoubleType() || t2.isFloatType()) {
                return Optional.of(DoubleType.INSTANCE);
            }
        }

        if (t1.isDecimalV2Type() || t2.isDecimalV2Type()) {
            return Optional.of(DecimalV2Type.widerDecimalV2Type(DecimalV2Type.forType(t1), DecimalV2Type.forType(t2)));
        }

        // date-like type
        if (t1.isDateTimeV2Type() && t2.isDateTimeV2Type()) {
            return Optional.of(DateTimeV2Type.getWiderDatetimeV2Type((DateTimeV2Type) t1, (DateTimeV2Type) t2));
        }
        if (t1.isDateLikeType() && t2.isDateLikeType()) {
            if (t1.isDateTimeV2Type()) {
                return Optional.of(t1);
            }
            if (t2.isDateTimeV2Type()) {
                return Optional.of(t2);
            }
            if (t1.isDateV2Type() || t2.isDateV2Type()) {
                // datev2 vs datetime
                if (t1.isDateTimeType() || t2.isDateTimeType()) {
                    return Optional.of(DateTimeV2Type.SYSTEM_DEFAULT);
                }
                // datev2 vs date
                return Optional.of(DateV2Type.INSTANCE);
            }
            // date vs datetime
            return Optional.of(DateTimeType.INSTANCE);
        }
        if (t1.isDateLikeType() || t2.isDateLikeType()) {
            DataType dateType = t1;
            DataType otherType = t2;
            if (t2.isDateLikeType()) {
                dateType = t2;
                otherType = t1;
            }
            if (dateType.isDateType() || dateType.isDateV2Type()) {
                if (otherType.isIntegerType() || otherType.isBigIntType() || otherType.isLargeIntType()) {
                    return Optional.of(otherType);
                }
            }
            if (dateType.isDateTimeType() || dateType.isDateTimeV2Type()) {
                if (otherType.isLargeIntType() || otherType.isDoubleType()) {
                    return Optional.of(otherType);
                }
            }
            return Optional.empty();
        }

        // time-like vs all other type
        if (t1.isTimeType() && t2.isTimeType()) {
            return Optional.of(TimeV2Type.INSTANCE);
        }
        if (t1.isTimeType() || t2.isTimeType()) {
            if (t1.isNumericType() || t2.isNumericType() || t1.isBooleanType() || t2.isBooleanType()) {
                return Optional.of(DoubleType.INSTANCE);
            }
            return Optional.empty();
        }

        // string-like, null, objected, decimal, date-like and time already processed
        // so only need to process numeric type without decimal plus boolean.
        if ((t1.isFloatType() && t2.isLargeIntType()) || (t1.isLargeIntType() && t2.isFloatType())) {
            return Optional.of(DoubleType.INSTANCE);
        }
        for (DataType dataType : NUMERIC_PRECEDENCE) {
            if (t1.equals(dataType) || t2.equals(dataType)) {
                return Optional.of(dataType);
            }
        }

        return Optional.empty();
    }

    private static Expression processDecimalV3BinaryArithmetic(BinaryArithmetic binaryArithmetic,
            Expression left, Expression right) {
        DecimalV3Type dt1 =
                DecimalV3Type.forType(TypeCoercionUtils.getNumResultType(left.getDataType()));
        DecimalV3Type dt2 =
                DecimalV3Type.forType(TypeCoercionUtils.getNumResultType(right.getDataType()));

        // check return type whether overflow, if true, turn to double
        DecimalV3Type retType = binaryArithmetic.getDataTypeForDecimalV3(dt1, dt2);

        // add, subtract and mod should cast children to exactly same type as return type
        if (binaryArithmetic instanceof Add || binaryArithmetic instanceof Subtract
                || binaryArithmetic instanceof Mod) {
            return castChildren(binaryArithmetic, left, right, retType);
        }
        // multiply do not need to cast children to same type
        return binaryArithmetic.withChildren(castIfNotSameType(left, dt1),
                castIfNotSameType(right, dt2));
    }

    /**
     * get min and max value of a data type
     *
     * @param dataType specific data type
     * @return min and max values pair
     */
    public static Optional<Pair<BigDecimal, BigDecimal>> getDataTypeMinMaxValue(DataType dataType) {
        if (dataType.isTinyIntType()) {
            return Optional.of(Pair.of(new BigDecimal(Byte.MIN_VALUE), new BigDecimal(Byte.MAX_VALUE)));
        } else if (dataType.isSmallIntType()) {
            return Optional.of(Pair.of(new BigDecimal(Short.MIN_VALUE), new BigDecimal(Short.MAX_VALUE)));
        } else if (dataType.isIntegerType()) {
            return Optional.of(Pair.of(new BigDecimal(Integer.MIN_VALUE), new BigDecimal(Integer.MAX_VALUE)));
        } else if (dataType.isBigIntType()) {
            return Optional.of(Pair.of(new BigDecimal(Long.MIN_VALUE), new BigDecimal(Long.MAX_VALUE)));
        } else if (dataType.isLargeIntType()) {
            return Optional.of(Pair.of(new BigDecimal(LargeIntType.MIN_VALUE), new BigDecimal(LargeIntType.MAX_VALUE)));
        } else if (dataType.isFloatType()) {
            return Optional.of(Pair.of(BigDecimal.valueOf(-Float.MAX_VALUE), new BigDecimal(Float.MAX_VALUE)));
        } else if (dataType.isDoubleType()) {
            return Optional.of(Pair.of(BigDecimal.valueOf(-Double.MAX_VALUE), new BigDecimal(Double.MAX_VALUE)));
        } else if (dataType.isDecimalV3Type()) {
            DecimalV3Type type = (DecimalV3Type) dataType;
            int precision = type.getPrecision();
            int scale = type.getScale();
            if (scale >= 0) {
                StringBuilder sb = new StringBuilder();
                sb.append(StringUtils.repeat('9', precision - scale));
                if (sb.length() == 0) {
                    sb.append('0');
                }
                if (scale > 0) {
                    sb.append('.');
                    sb.append(StringUtils.repeat('9', scale));
                }
                return Optional.of(Pair.of(new BigDecimal("-" + sb.toString()), new BigDecimal(sb.toString())));
            }
        }

        return Optional.empty();
    }

    private static boolean supportCompare(DataType dataType) {
        if (dataType.isArrayType()) {
            return true;
        }
        if (!(dataType instanceof PrimitiveType)) {
            return false;
        }
        if (dataType.isObjectType()) {
            return false;
        }
        if (dataType instanceof JsonType) {
            return false;
        }
        return true;
    }
}
