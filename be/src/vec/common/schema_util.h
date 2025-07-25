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

#pragma once

#include <gen_cpp/Descriptors_types.h>
#include <parallel_hashmap/phmap.h>
#include <stddef.h>
#include <stdint.h>

#include <map>
#include <mutex>
#include <string>

#include "common/status.h"
#include "olap/tablet_schema.h"
#include "udf/udf.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_variant.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"

namespace doris {
enum class FieldType;

namespace vectorized {
class Block;
class IColumn;
struct ColumnWithTypeAndName;
struct ParseConfig;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized::schema_util {
/// Returns number of dimensions in Array type. 0 if type is not array.
size_t get_number_of_dimensions(const IDataType& type);

/// Returns number of dimensions in Array column. 0 if column is not array.
size_t get_number_of_dimensions(const IColumn& column);

/// Returns type of scalars of Array of arbitrary dimensions.
DataTypePtr get_base_type_of_array(const DataTypePtr& type);

/// Returns Array with requested number of dimensions and no scalars.
Array create_empty_array_field(size_t num_dimensions);

// Cast column to dst type
Status cast_column(const ColumnWithTypeAndName& arg, const DataTypePtr& type, ColumnPtr* result);

/// If both of types are signed/unsigned integers and size of left field type
/// is less than right type, we don't need to convert field,
/// because all integer fields are stored in Int64/UInt64.
bool is_conversion_required_between_integers(const PrimitiveType& lhs, const PrimitiveType& rhs);

struct ExtraInfo {
    // -1 indicates it's not a Frontend generated column
    int32_t unique_id = -1;
    int32_t parent_unique_id = -1;
    vectorized::PathInData path_info;
};

TabletColumn get_column_by_type(const vectorized::DataTypePtr& data_type, const std::string& name,
                                const ExtraInfo& ext_info);

// three steps to parse and encode variant columns into flatterned columns
// 1. parse variant from raw json string
// 2. finalize variant column to each subcolumn least commn types, default ignore sparse sub columns
// 3. encode sparse sub columns
Status parse_variant_columns(Block& block, const std::vector<int>& variant_pos,
                             const ParseConfig& config);
Status encode_variant_sparse_subcolumns(ColumnVariant& column);

// check if the tuple_paths has ambiguous paths
// situation:
// throw exception if there exists a prefix with matched names, but not matched structure (is Nested, number of dimensions).
Status check_variant_has_no_ambiguous_paths(const std::vector<PathInData>& paths);

// Pick the tablet schema with the highest schema version as the reference.
// Then update all variant columns to there least common types.
// Return the final merged schema as common schema.
// If base_schema == nullptr then, max schema version tablet schema will be picked as base schema
Status get_least_common_schema(const std::vector<TabletSchemaSPtr>& schemas,
                               const TabletSchemaSPtr& base_schema, TabletSchemaSPtr& result,
                               bool check_schema_size = false);

// Get least common types for extracted columns which has Path info,
// with a speicified variant column's unique id
Status update_least_common_schema(const std::vector<TabletSchemaSPtr>& schemas,
                                  TabletSchemaSPtr& common_schema, int32_t variant_col_unique_id,
                                  std::unordered_set<PathInData, PathInData::Hash>* path_set);

Status update_least_sparse_column(const std::vector<TabletSchemaSPtr>& schemas,
                                  TabletSchemaSPtr& common_schema, int32_t variant_col_unique_id,
                                  const std::unordered_set<PathInData, PathInData::Hash>& path_set);

// inherit attributes like index/agg info from it's parent column
void inherit_column_attributes(TabletSchemaSPtr& schema);

// source: variant column
// target: extracted column from variant column
void inherit_column_attributes(const TabletColumn& source, TabletColumn& target,
                               TabletSchemaSPtr& target_schema);

// get sorted subcolumns of variant
vectorized::ColumnVariant::Subcolumns get_sorted_subcolumns(
        const vectorized::ColumnVariant::Subcolumns& subcolumns);

// Extract json data from source with path
Status extract(ColumnPtr source, const PathInData& path, MutableColumnPtr& dst);

std::string dump_column(DataTypePtr type, const ColumnPtr& col);

bool has_schema_index_diff(const TabletSchema* new_schema, const TabletSchema* old_schema,
                           int32_t new_col_idx, int32_t old_col_idx);

} // namespace  doris::vectorized::schema_util
