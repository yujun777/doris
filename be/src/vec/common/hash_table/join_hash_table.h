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

#include <gen_cpp/PlanNodes_types.h>

#include <limits>

#include "common/exception.h"
#include "common/status.h"
#include "vec/columns/column_filter_helper.h"
#include "vec/common/custom_allocator.h"
#include "vec/common/hash_table/hash.h"

namespace doris {
#include "common/compile_check_begin.h"
template <typename Key, typename Hash = DefaultHash<Key>>
class JoinHashTable {
public:
    using key_type = Key;
    using mapped_type = void*;
    using value_type = void*;
    size_t hash(const Key& x) const { return Hash()(x); }

    static uint32_t calc_bucket_size(size_t num_elem) {
        size_t expect_bucket_size = num_elem + (num_elem - 1) / 7;
        return (uint32_t)std::min(phmap::priv::NormalizeCapacity(expect_bucket_size) + 1,
                                  static_cast<size_t>(std::numeric_limits<int32_t>::max()) + 1);
    }

    size_t get_byte_size() const {
        auto cal_vector_mem = [](const auto& vec) { return vec.capacity() * sizeof(vec[0]); };
        return cal_vector_mem(visited) + cal_vector_mem(first) + cal_vector_mem(next);
    }

    template <int JoinOpType>
    void prepare_build(size_t num_elem, int batch_size, bool has_null_key) {
        _has_null_key = has_null_key;

        // the first row in build side is not really from build side table
        _empty_build_side = num_elem <= 1;
        max_batch_size = batch_size;
        bucket_size = calc_bucket_size(num_elem + 1);
        first.resize(bucket_size + 1);
        next.resize(num_elem);

        if constexpr (JoinOpType == TJoinOp::FULL_OUTER_JOIN ||
                      JoinOpType == TJoinOp::RIGHT_OUTER_JOIN ||
                      JoinOpType == TJoinOp::RIGHT_ANTI_JOIN ||
                      JoinOpType == TJoinOp::RIGHT_SEMI_JOIN) {
            visited.resize(num_elem);
        }
    }

    uint32_t get_bucket_size() const { return bucket_size; }

    size_t size() const { return next.size(); }

    DorisVector<uint8_t>& get_visited() { return visited; }

    bool empty_build_side() const { return _empty_build_side; }

    void build(const Key* __restrict keys, const uint32_t* __restrict bucket_nums,
               uint32_t num_elem, bool keep_null_key) {
        build_keys = keys;
        for (uint32_t i = 1; i < num_elem; i++) {
            uint32_t bucket_num = bucket_nums[i];
            next[i] = first[bucket_num];
            first[bucket_num] = i;
        }
        if (!keep_null_key) {
            first[bucket_size] = 0; // index = bucket_size means null
        }
        _keep_null_key = keep_null_key;
    }

    template <int JoinOpType>
    auto find_batch(const Key* __restrict keys, const uint32_t* __restrict build_idx_map,
                    int probe_idx, uint32_t build_idx, int probe_rows,
                    uint32_t* __restrict probe_idxs, bool& probe_visited,
                    uint32_t* __restrict build_idxs, const uint8_t* null_map,
                    bool with_other_conjuncts, bool is_mark_join, bool has_mark_join_conjunct) {
        if ((JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
             JoinOpType == TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN) &&
            _empty_build_side) {
            return _process_null_aware_left_half_join_for_empty_build_side<JoinOpType>(
                    probe_idx, probe_rows, probe_idxs, build_idxs);
        }

        if (with_other_conjuncts) {
            return _find_batch_conjunct<JoinOpType, false>(
                    keys, build_idx_map, probe_idx, build_idx, probe_rows, probe_idxs, build_idxs);
        }

        if (is_mark_join) {
            bool is_null_aware_join = JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
                                      JoinOpType == TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN;
            bool is_left_half_join =
                    JoinOpType == TJoinOp::LEFT_SEMI_JOIN || JoinOpType == TJoinOp::LEFT_ANTI_JOIN;

            /// For null aware join or left half(semi/anti) join without other conjuncts and without
            /// mark join conjunct.
            /// If one row on probe side has one match in build side, we should stop searching the
            /// hash table for this row.
            if (is_null_aware_join || (is_left_half_join && !has_mark_join_conjunct)) {
                return _find_batch_conjunct<JoinOpType, true>(keys, build_idx_map, probe_idx,
                                                              build_idx, probe_rows, probe_idxs,
                                                              build_idxs);
            }

            return _find_batch_conjunct<JoinOpType, false>(
                    keys, build_idx_map, probe_idx, build_idx, probe_rows, probe_idxs, build_idxs);
        }

        if (JoinOpType == TJoinOp::INNER_JOIN || JoinOpType == TJoinOp::FULL_OUTER_JOIN ||
            JoinOpType == TJoinOp::LEFT_OUTER_JOIN || JoinOpType == TJoinOp::RIGHT_OUTER_JOIN) {
            return _find_batch_inner_outer_join<JoinOpType>(keys, build_idx_map, probe_idx,
                                                            build_idx, probe_rows, probe_idxs,
                                                            probe_visited, build_idxs);
        }
        if (JoinOpType == TJoinOp::LEFT_ANTI_JOIN || JoinOpType == TJoinOp::LEFT_SEMI_JOIN ||
            JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
            if (null_map) {
                return _find_batch_left_semi_anti<JoinOpType, true>(
                        keys, build_idx_map, probe_idx, probe_rows, probe_idxs, null_map);
            } else {
                return _find_batch_left_semi_anti<JoinOpType, false>(
                        keys, build_idx_map, probe_idx, probe_rows, probe_idxs, nullptr);
            }
        }
        if (JoinOpType == TJoinOp::RIGHT_ANTI_JOIN || JoinOpType == TJoinOp::RIGHT_SEMI_JOIN) {
            return _find_batch_right_semi_anti(keys, build_idx_map, probe_idx, probe_rows);
        }
        throw Exception(ErrorCode::INTERNAL_ERROR, "meet invalid hash join input");
    }

    /**
     * Because the equality comparison result of null with any value is null,
     * in null aware join, if the probe key of a row in the left table(probe side) is null,
     * then this row will match all rows on the right table(build side) (the match result is null).
     * If the probe key of a row in the left table does not match any row in right table,
     * this row will match all rows with null key in the right table.
     * select 'a' in ('b', null) => 'a' = 'b' or 'a' = null => false or null => null
     * select 'a' in ('a', 'b', null) => true
     * select 'a' not in ('b', null) => null => 'a' != 'b' and 'a' != null => true and null => null
     * select 'a' not in ('a', 'b', null) => false
     */
    auto find_null_aware_with_other_conjuncts(const Key* __restrict keys,
                                              const uint32_t* __restrict build_idx_map,
                                              int probe_idx, uint32_t build_idx, int probe_rows,
                                              uint32_t* __restrict probe_idxs,
                                              uint32_t* __restrict build_idxs,
                                              uint8_t* __restrict null_flags,
                                              bool picking_null_keys, const uint8_t* null_map) {
        if (null_map) {
            return _find_null_aware_with_other_conjuncts_impl<true>(
                    keys, build_idx_map, probe_idx, build_idx, probe_rows, probe_idxs, build_idxs,
                    null_flags, picking_null_keys, null_map);
        } else {
            return _find_null_aware_with_other_conjuncts_impl<false>(
                    keys, build_idx_map, probe_idx, build_idx, probe_rows, probe_idxs, build_idxs,
                    null_flags, picking_null_keys, nullptr);
        }
    }

    template <int JoinOpType, bool is_mark_join>
    bool iterate_map(vectorized::ColumnOffset32& build_idxs,
                     vectorized::ColumnFilterHelper* mark_column_helper) const {
        const auto batch_size = max_batch_size;
        const auto elem_num = visited.size();
        int count = 0;
        build_idxs.resize(batch_size);

        while (count < batch_size && iter_idx < elem_num) {
            const auto matched = visited[iter_idx];
            build_idxs.get_element(count) = iter_idx;
            if constexpr (JoinOpType == TJoinOp::RIGHT_SEMI_JOIN) {
                if constexpr (is_mark_join) {
                    mark_column_helper->insert_value(matched);
                    ++count;
                } else {
                    count += matched;
                }
            } else {
                count += !matched;
            }
            iter_idx++;
        }

        build_idxs.resize(count);
        return iter_idx >= elem_num;
    }

    bool has_null_key() { return _has_null_key; }

    bool keep_null_key() { return _keep_null_key; }

    void pre_build_idxs(DorisVector<uint32_t>& buckets) const {
        for (unsigned int& bucket : buckets) {
            bucket = first[bucket];
        }
    }

private:
    template <int JoinOpType>
    auto _process_null_aware_left_half_join_for_empty_build_side(int probe_idx, int probe_rows,
                                                                 uint32_t* __restrict probe_idxs,
                                                                 uint32_t* __restrict build_idxs) {
        if (JoinOpType != TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN &&
            JoinOpType != TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "process_null_aware_left_half_join_for_empty_build_side meet invalid "
                            "hash join input");
        }
        uint32_t matched_cnt = 0;
        const auto batch_size = max_batch_size;

        while (probe_idx < probe_rows && matched_cnt < batch_size) {
            probe_idxs[matched_cnt] = probe_idx++;
            build_idxs[matched_cnt] = 0;
            ++matched_cnt;
        }

        return std::tuple {probe_idx, 0U, matched_cnt};
    }

    auto _find_batch_right_semi_anti(const Key* __restrict keys,
                                     const uint32_t* __restrict build_idx_map, int probe_idx,
                                     int probe_rows) {
        while (probe_idx < probe_rows) {
            auto build_idx = build_idx_map[probe_idx];

            while (build_idx) {
                if (!visited[build_idx] && keys[probe_idx] == build_keys[build_idx]) {
                    visited[build_idx] = 1;
                }
                build_idx = next[build_idx];
            }
            probe_idx++;
        }
        return std::tuple {probe_idx, 0U, 0U};
    }

    template <int JoinOpType, bool has_null_map>
    auto _find_batch_left_semi_anti(const Key* __restrict keys,
                                    const uint32_t* __restrict build_idx_map, int probe_idx,
                                    int probe_rows, uint32_t* __restrict probe_idxs,
                                    const uint8_t* null_map) {
        uint32_t matched_cnt = 0;
        const auto batch_size = max_batch_size;

        while (probe_idx < probe_rows && matched_cnt < batch_size) {
            if constexpr (JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN && has_null_map) {
                if (null_map[probe_idx]) {
                    probe_idx++;
                    continue;
                }
            }

            auto build_idx = build_idx_map[probe_idx];

            while (build_idx && keys[probe_idx] != build_keys[build_idx]) {
                build_idx = next[build_idx];
            }
            bool matched = JoinOpType == TJoinOp::LEFT_SEMI_JOIN ? build_idx != 0 : build_idx == 0;
            probe_idxs[matched_cnt] = probe_idx++;
            matched_cnt += matched;
        }
        return std::tuple {probe_idx, 0U, matched_cnt};
    }

    template <int JoinOpType, bool only_need_to_match_one>
    auto _find_batch_conjunct(const Key* __restrict keys, const uint32_t* __restrict build_idx_map,
                              int probe_idx, uint32_t build_idx, int probe_rows,
                              uint32_t* __restrict probe_idxs, uint32_t* __restrict build_idxs) {
        uint32_t matched_cnt = 0;
        const auto batch_size = max_batch_size;

        auto do_the_probe = [&]() {
            while (build_idx && matched_cnt < batch_size) {
                if (keys[probe_idx] == build_keys[build_idx]) {
                    build_idxs[matched_cnt] = build_idx;
                    probe_idxs[matched_cnt] = probe_idx;
                    matched_cnt++;

                    if constexpr (only_need_to_match_one) {
                        build_idx = 0;
                        break;
                    }
                }
                build_idx = next[build_idx];
            }

            if constexpr (JoinOpType == TJoinOp::LEFT_OUTER_JOIN ||
                          JoinOpType == TJoinOp::FULL_OUTER_JOIN ||
                          JoinOpType == TJoinOp::LEFT_SEMI_JOIN ||
                          JoinOpType == TJoinOp::LEFT_ANTI_JOIN ||
                          JoinOpType == doris::TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
                          JoinOpType == doris::TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN) {
                // may over batch_size when emplace 0 into build_idxs
                if (!build_idx) {
                    probe_idxs[matched_cnt] = probe_idx;
                    build_idxs[matched_cnt] = 0;
                    matched_cnt++;
                }
            }

            probe_idx++;
        };

        if (build_idx) {
            do_the_probe();
        }

        while (probe_idx < probe_rows && matched_cnt < batch_size) {
            build_idx = build_idx_map[probe_idx];
            do_the_probe();
        }

        probe_idx -= (build_idx != 0);
        return std::tuple {probe_idx, build_idx, matched_cnt};
    }

    template <int JoinOpType>
    auto _find_batch_inner_outer_join(const Key* __restrict keys,
                                      const uint32_t* __restrict build_idx_map, int probe_idx,
                                      uint32_t build_idx, int probe_rows,
                                      uint32_t* __restrict probe_idxs, bool& probe_visited,
                                      uint32_t* __restrict build_idxs) {
        uint32_t matched_cnt = 0;
        const auto batch_size = max_batch_size;

        auto do_the_probe = [&]() {
            while (build_idx && matched_cnt < batch_size) {
                if (keys[probe_idx] == build_keys[build_idx]) {
                    probe_idxs[matched_cnt] = probe_idx;
                    build_idxs[matched_cnt] = build_idx;
                    matched_cnt++;
                    if constexpr (JoinOpType == TJoinOp::RIGHT_OUTER_JOIN ||
                                  JoinOpType == TJoinOp::FULL_OUTER_JOIN) {
                        if (!visited[build_idx]) {
                            visited[build_idx] = 1;
                        }
                    }
                }
                build_idx = next[build_idx];
            }

            if constexpr (JoinOpType == TJoinOp::LEFT_OUTER_JOIN ||
                          JoinOpType == TJoinOp::FULL_OUTER_JOIN) {
                // `(!matched_cnt || probe_idxs[matched_cnt - 1] != probe_idx)` means not match one build side
                probe_visited |= (matched_cnt && probe_idxs[matched_cnt - 1] == probe_idx);
                if (!build_idx) {
                    if (!probe_visited) {
                        probe_idxs[matched_cnt] = probe_idx;
                        build_idxs[matched_cnt] = 0;
                        matched_cnt++;
                    }
                    probe_visited = false;
                }
            }
            probe_idx++;
        };

        if (build_idx) {
            do_the_probe();
        }

        while (probe_idx < probe_rows && matched_cnt < batch_size) {
            build_idx = build_idx_map[probe_idx];
            do_the_probe();
        }

        probe_idx -= (build_idx != 0);
        return std::tuple {probe_idx, build_idx, matched_cnt};
    }

    template <bool has_null_map>
    auto _find_null_aware_with_other_conjuncts_impl(
            const Key* __restrict keys, const uint32_t* __restrict build_idx_map, int probe_idx,
            uint32_t build_idx, int probe_rows, uint32_t* __restrict probe_idxs,
            uint32_t* __restrict build_idxs, uint8_t* __restrict null_flags, bool picking_null_keys,
            const uint8_t* null_map) {
        uint32_t matched_cnt = 0;
        const auto batch_size = max_batch_size;

        auto do_the_probe = [&]() {
            /// If no any rows match the probe key, here start to handle null keys in build side.
            /// The result of "Any = null" is null.
            if (build_idx == 0 && !picking_null_keys) {
                build_idx = first[bucket_size];
                picking_null_keys = true; // now pick null from build side
            }

            while (build_idx && matched_cnt < batch_size) {
                if (picking_null_keys || keys[probe_idx] == build_keys[build_idx]) {
                    build_idxs[matched_cnt] = build_idx;
                    probe_idxs[matched_cnt] = probe_idx;
                    null_flags[matched_cnt] = picking_null_keys;
                    matched_cnt++;
                }

                build_idx = next[build_idx];

                // If `build_idx` is 0, all matched keys are handled,
                // now need to handle null keys in build side.
                if (!build_idx && !picking_null_keys) {
                    build_idx = first[bucket_size];
                    picking_null_keys = true; // now pick null keys from build side
                }
            }

            // may over batch_size when emplace 0 into build_idxs
            if (!build_idx) {
                probe_idxs[matched_cnt] = probe_idx;
                build_idxs[matched_cnt] = 0;
                picking_null_keys = false;
                matched_cnt++;
            }

            probe_idx++;
        };

        if (build_idx) {
            do_the_probe();
        }

        while (probe_idx < probe_rows && matched_cnt < batch_size) {
            build_idx = build_idx_map[probe_idx];

            /// If the probe key is null
            if constexpr (has_null_map) {
                if (null_map[probe_idx]) {
                    probe_idx++;
                    break;
                }
            }
            do_the_probe();
            if (picking_null_keys) {
                break;
            }
        }

        probe_idx -= (build_idx != 0);
        return std::tuple {probe_idx, build_idx, matched_cnt, picking_null_keys};
    }

    const Key* __restrict build_keys;
    DorisVector<uint8_t> visited;

    uint32_t bucket_size = 1;
    int max_batch_size = 4064;

    DorisVector<uint32_t> first = {0};
    DorisVector<uint32_t> next = {0};

    // use in iter hash map
    mutable uint32_t iter_idx = 1;
    bool _has_null_key = false;
    bool _keep_null_key = false;
    bool _empty_build_side = true;
};

template <typename Key, typename Hash = DefaultHash<Key>>
using JoinHashMap = JoinHashTable<Key, Hash>;
#include "common/compile_check_end.h"
} // namespace doris