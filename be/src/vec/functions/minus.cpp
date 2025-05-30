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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/Minus.cpp
// and modified by Doris

#include <utility>

#include "runtime/decimalv2_value.h"
#include "vec/common/arithmetic_overflow.h"
#include "vec/data_types/number_traits.h"
#include "vec/functions/function_binary_arithmetic.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

template <typename A, typename B>
struct MinusImpl {
    static constexpr PrimitiveType ResultType = NumberTraits::ResultOfSubtraction<A, B>::Type;
    static const constexpr bool allow_decimal = true;

    template <typename Result = typename PrimitiveTypeTraits<ResultType>::CppNativeType>
    static inline Result apply(A a, B b) {
        return static_cast<Result>(a) - b;
    }

    template <typename Result = DecimalV2Value>
    static inline DecimalV2Value apply(const DecimalV2Value& a, const DecimalV2Value& b) {
        return DecimalV2Value(a.value() - b.value());
    }

    /// Apply operation and check overflow. It's used for Decimal operations. @returns true if overflowed, false otherwise.
    template <typename Result = typename PrimitiveTypeTraits<ResultType>::CppNativeType>
    static inline bool apply(A a, B b, Result& c) {
        return common::sub_overflow(static_cast<Result>(a), b, c);
    }
};

struct NameMinus {
    static constexpr auto name = "subtract";
};
using FunctionMinus = FunctionBinaryArithmetic<MinusImpl, NameMinus, false>;

void register_function_minus(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionMinus>();
}
} // namespace doris::vectorized
