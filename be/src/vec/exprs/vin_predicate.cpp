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

#include "vec/exprs/vin_predicate.h"

#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstddef>
#include <ostream>

#include "common/status.h"
#include "runtime/runtime_state.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class RowDescriptor;
class RuntimeState;
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"

VInPredicate::VInPredicate(const TExprNode& node)
        : VExpr(node), _is_not_in(node.in_predicate.is_not_in) {}

Status VInPredicate::prepare(RuntimeState* state, const RowDescriptor& desc,
                             VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));

    if (_children.empty()) {
        return Status::InternalError("no Function operator in.");
    }

    _expr_name =
            fmt::format("({} {} set)", _children[0]->expr_name(), _is_not_in ? "not_in" : "in");

    ColumnsWithTypeAndName argument_template;
    argument_template.reserve(get_num_children());
    for (auto child : _children) {
        argument_template.emplace_back(nullptr, child->data_type(), child->expr_name());
    }

    // construct the proper function_name
    std::string head(_is_not_in ? "not_" : "");
    std::string real_function_name = head + std::string(function_name);
    auto arg_type = remove_nullable(argument_template[0].type);
    if (is_complex_type(arg_type->get_primitive_type())) {
        real_function_name = "collection_" + real_function_name;
    }
    _function = SimpleFunctionFactory::instance().get_function(
            real_function_name, argument_template, _data_type,
            {.enable_decimal256 = state->enable_decimal256()});
    if (_function == nullptr) {
        return Status::NotSupported("Function {} is not implemented", real_function_name);
    }

    VExpr::register_function_context(state, context);
    _prepare_finished = true;

    if (state->query_options().__isset.in_list_value_count_threshold) {
        _in_list_value_count_threshold = state->query_options().in_list_value_count_threshold;
    }
    return Status::OK();
}

Status VInPredicate::open(RuntimeState* state, VExprContext* context,
                          FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    for (auto& child : _children) {
        RETURN_IF_ERROR(child->open(state, context, scope));
    }
    RETURN_IF_ERROR(VExpr::init_function_context(state, context, scope, _function));
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        RETURN_IF_ERROR(VExpr::get_const_col(context, nullptr));
    }

    _is_args_all_constant = std::all_of(_children.begin() + 1, _children.end(),
                                        [](const VExprSPtr& expr) { return expr->is_constant(); });
    _open_finished = true;
    return Status::OK();
}

void VInPredicate::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
    VExpr::close_function_context(context, scope, _function);
    VExpr::close(context, scope);
}

Status VInPredicate::evaluate_inverted_index(VExprContext* context, uint32_t segment_num_rows) {
    DCHECK_GE(get_num_children(), 2);
    return _evaluate_inverted_index(context, _function, segment_num_rows);
}

Status VInPredicate::execute(VExprContext* context, Block* block, int* result_column_id) {
    if (is_const_and_have_executed()) { // const have execute in open function
        return get_result_from_const(block, _expr_name, result_column_id);
    }
    if (fast_execute(context, block, result_column_id)) {
        return Status::OK();
    }
    DCHECK(_open_finished || _getting_const_col);

    // This is an optimization. For expressions like colA IN (1, 2, 3, 4),
    // where all values inside the IN clause are constants,
    // a hash set is created during open, and it will not be accessed again during execute
    //  Here, _children[0] is colA
    const size_t args_size = _is_args_all_constant ? 1 : _children.size();

    doris::vectorized::ColumnNumbers arguments(args_size);
    for (int i = 0; i < args_size; ++i) {
        int column_id = -1;
        RETURN_IF_ERROR(_children[i]->execute(context, block, &column_id));
        arguments[i] = column_id;
    }
    // call function
    uint32_t num_columns_without_result = block->columns();
    // prepare a column to save result
    block->insert({nullptr, _data_type, _expr_name});

    RETURN_IF_ERROR(_function->execute(context->fn_context(_fn_context_index), *block, arguments,
                                       num_columns_without_result, block->rows(), false));
    *result_column_id = num_columns_without_result;
    return Status::OK();
}

size_t VInPredicate::estimate_memory(const size_t rows) {
    if (is_const_and_have_executed()) {
        return 0;
    }

    size_t estimate_size = 0;

    for (int i = 0; i < _children.size(); ++i) {
        estimate_size += _children[i]->estimate_memory(rows);
    }

    if (_data_type->is_nullable()) {
        estimate_size += rows * sizeof(uint8_t);
    }

    estimate_size += rows * sizeof(uint8_t);

    return estimate_size;
}

const std::string& VInPredicate::expr_name() const {
    return _expr_name;
}

std::string VInPredicate::debug_string() const {
    std::stringstream out;
    out << "InPredicate(" << children()[0]->debug_string() << " " << _is_not_in << ",[";
    int num_children = get_num_children();

    for (int i = 1; i < num_children; ++i) {
        out << (i == 1 ? "" : " ") << children()[i]->debug_string();
    }

    out << "])";
    return out.str();
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
