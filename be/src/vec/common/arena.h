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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/Arena.h
// and modified by Doris

#pragma once

#include <common/compiler_util.h>
#include <sanitizer/asan_interface.h>
#include <string.h>

#include <boost/noncopyable.hpp>
#include <memory>
#include <vector>

#include "vec/common/allocator.h"
#include "vec/common/allocator_fwd.h"

namespace doris::vectorized {

/** Memory pool to append something. For example, short strings.
  * Usage scenario:
  * - put lot of strings inside pool, keep their addresses;
  * - addresses remain valid during lifetime of pool;
  * - at destruction of pool, all memory is freed;
  * - memory is allocated and freed by large chunks;
  * - freeing parts of data is not possible (but look at ArenaWithFreeLists if you need);
  */
class Arena : private boost::noncopyable {
private:
    /// Padding allows to use 'memcpy_small_allow_read_write_overflow15' instead of 'memcpy'.
    static constexpr size_t pad_right = 15;

    /// Contiguous chunk of memory and pointer to free space inside it. Member of single-linked list.
    struct alignas(16) Chunk : private Allocator<false> /// empty base optimization
    {
        char* begin = nullptr;
        char* pos = nullptr;
        char* end = nullptr; /// does not include padding.

        Chunk* prev = nullptr;

        Chunk(size_t size_, Chunk* prev_) {
            begin = reinterpret_cast<char*>(Allocator<false>::alloc(size_));
            pos = begin;
            end = begin + size_ - pad_right;
            prev = prev_;

            ASAN_POISON_MEMORY_REGION(begin, size_);
        }

        ~Chunk() {
            /// We must unpoison the memory before returning to the allocator,
            /// because the allocator might not have asan integration, and the
            /// memory would stay poisoned forever. If the allocator supports
            /// asan, it will correctly poison the memory by itself.
            ASAN_UNPOISON_MEMORY_REGION(begin, size());

            Allocator<false>::free(begin, size());

            if (prev) delete prev;
        }

        size_t size() const { return end + pad_right - begin; }
        size_t remaining() const { return end - pos; }
        size_t used() const { return pos - begin; }
    };

    size_t growth_factor = 2;
    size_t linear_growth_threshold = 128 * 1024 * 1024;

    /// Last contiguous chunk of memory.
    Chunk* head = nullptr;
    size_t size_in_bytes = 0;
    size_t _initial_size = 4096;
    // The memory used by all chunks, excluding head.
    size_t _used_size_no_head = 0;

    static size_t round_up_to_page_size(size_t s) { return (s + 4096 - 1) / 4096 * 4096; }

    /// If chunks size is less than 'linear_growth_threshold', then use exponential growth, otherwise - linear growth
    ///  (to not allocate too much excessive memory).
    size_t next_size(size_t min_next_size) {
        DCHECK(head != nullptr);
        size_t size_after_grow = 0;

        if (head->size() < linear_growth_threshold) {
            size_after_grow = std::max(min_next_size, head->size() * growth_factor);
        } else {
            // alloc_continue() combined with linear growth results in quadratic
            // behavior: we append the data by small amounts, and when it
            // doesn't fit, we create a new chunk and copy all the previous data
            // into it. The number of times we do this is directly proportional
            // to the total size of data that is going to be serialized. To make
            // the copying happen less often, round the next size up to the
            // linear_growth_threshold.
            size_after_grow =
                    ((min_next_size + linear_growth_threshold - 1) / linear_growth_threshold) *
                    linear_growth_threshold;
        }

        assert(size_after_grow >= min_next_size);
        return round_up_to_page_size(size_after_grow);
    }

    /// Add next contiguous chunk of memory with size not less than specified.
    void NO_INLINE _add_chunk(size_t min_size) {
        DCHECK(head != nullptr);
        _used_size_no_head += head->used();
        head = new Chunk(next_size(min_size + pad_right), head);
        size_in_bytes += head->size();
    }

    void _init_head_if_needed() {
        if (UNLIKELY(head == nullptr)) {
            head = new Chunk(_initial_size, nullptr);
            size_in_bytes += head->size();
        }
    }

public:
    Arena(size_t initial_size_ = 4096, size_t growth_factor_ = 2,
          size_t linear_growth_threshold_ = 128 * 1024 * 1024)
            : growth_factor(growth_factor_),
              linear_growth_threshold(linear_growth_threshold_),
              _initial_size(initial_size_),
              _used_size_no_head(0) {}

    ~Arena() { delete head; }

    /// Get piece of memory, without alignment.
    char* alloc(size_t size) {
        _init_head_if_needed();

        if (UNLIKELY(head->pos + size > head->end)) {
            _add_chunk(size);
        }

        char* res = head->pos;
        head->pos += size;
        ASAN_UNPOISON_MEMORY_REGION(res, size + pad_right);
        return res;
    }

    /// Get piece of memory with alignment
    char* aligned_alloc(size_t size, size_t alignment) {
        _init_head_if_needed();

        do {
            void* head_pos = head->pos;
            size_t space = head->end - head->pos;

            auto res = static_cast<char*>(std::align(alignment, size, head_pos, space));
            if (res) {
                head->pos = static_cast<char*>(head_pos);
                head->pos += size;
                ASAN_UNPOISON_MEMORY_REGION(res, size + pad_right);
                return res;
            }

            _add_chunk(size + alignment);
        } while (true);
    }

    template <typename T>
    T* alloc() {
        return reinterpret_cast<T*>(aligned_alloc(sizeof(T), alignof(T)));
    }

    /** Rollback just performed allocation.
      * Must pass size not more that was just allocated.
	  * Return the resulting head pointer, so that the caller can assert that
	  * the allocation it intended to roll back was indeed the last one.
      */
    void* rollback(size_t size) {
        DCHECK(head != nullptr);

        head->pos -= size;
        ASAN_POISON_MEMORY_REGION(head->pos, size + pad_right);
        return head->pos;
    }

    /** Begin or expand a contiguous range of memory.
      * 'range_start' is the start of range. If nullptr, a new range is
      * allocated.
      * If there is no space in the current chunk to expand the range,
      * the entire range is copied to a new, bigger memory chunk, and the value
      * of 'range_start' is updated.
      * If the optional 'start_alignment' is specified, the start of range is
      * kept aligned to this value.
      *
      * NOTE This method is usable only for the last allocation made on this
      * Arena. For earlier allocations, see 'realloc' method.
      */
    [[nodiscard]] char* alloc_continue(size_t additional_bytes, char const*& range_start,
                                       size_t start_alignment = 0) {
        if (!range_start) {
            // Start a new memory range.
            char* result = start_alignment ? aligned_alloc(additional_bytes, start_alignment)
                                           : alloc(additional_bytes);

            range_start = result;
            return result;
        }

        DCHECK(head != nullptr);

        // Extend an existing memory range with 'additional_bytes'.

        // This method only works for extending the last allocation. For lack of
        // original size, check a weaker condition: that 'begin' is at least in
        // the current Chunk.
        assert(range_start >= head->begin && range_start < head->end);

        if (head->pos + additional_bytes <= head->end) {
            // The new size fits into the last chunk, so just alloc the
            // additional size. We can alloc without alignment here, because it
            // only applies to the start of the range, and we don't change it.
            return alloc(additional_bytes);
        }

        // New range doesn't fit into this chunk, will copy to a new one.
        //
        // Note: among other things, this method is used to provide a hack-ish
        // implementation of realloc over Arenas in ArenaAllocators. It wastes a
        // lot of memory -- quadratically so when we reach the linear allocation
        // threshold. This deficiency is intentionally left as is, and should be
        // solved not by complicating this method, but by rethinking the
        // approach to memory management for aggregate function states, so that
        // we can provide a proper realloc().
        const size_t existing_bytes = head->pos - range_start;
        const size_t new_bytes = existing_bytes + additional_bytes;
        const char* old_range = range_start;

        char* new_range =
                start_alignment ? aligned_alloc(new_bytes, start_alignment) : alloc(new_bytes);

        memcpy(new_range, old_range, existing_bytes);

        range_start = new_range;
        return new_range + existing_bytes;
    }

    /// NOTE Old memory region is wasted.
    [[nodiscard]] char* realloc(const char* old_data, size_t old_size, size_t new_size) {
        char* res = alloc(new_size);
        if (old_data) {
            memcpy(res, old_data, old_size);
            ASAN_POISON_MEMORY_REGION(old_data, old_size);
        }
        return res;
    }

    [[nodiscard]] char* aligned_realloc(const char* old_data, size_t old_size, size_t new_size,
                                        size_t alignment) {
        char* res = aligned_alloc(new_size, alignment);
        if (old_data) {
            memcpy(res, old_data, old_size);
            ASAN_POISON_MEMORY_REGION(old_data, old_size);
        }
        return res;
    }

    /// Insert string without alignment.
    [[nodiscard]] const char* insert(const char* data, size_t size) {
        char* res = alloc(size);
        memcpy(res, data, size);
        return res;
    }

    [[nodiscard]] const char* aligned_insert(const char* data, size_t size, size_t alignment) {
        char* res = aligned_alloc(size, alignment);
        memcpy(res, data, size);
        return res;
    }

    /**
    * Delete all the chunks before the head, usually the head is the largest chunk in the arena.
    * considering the scenario of memory reuse:
    * 1. first time, use arena alloc 64K memory, 4K each time, at this time, there are 4 chunks of 4k 8k 16k 32k in arena.
    * 2. then, clear arena, only one 32k chunk left in the arena.
    * 3. second time, same alloc 64K memory, there are 4 chunks of 4k 8k 16k 32k in arena.
    * 4. then, clear arena, only one 64k chunk left in the arena.
    * 5. third time, same alloc 64K memory, there is still only one 64K chunk in the arena, and the memory is fully reused.
    *
    * special case: if the chunk is larger than 128M, it will no longer be expanded by a multiple of 2.
    * If alloc 4G memory, 128M each time, then only one 128M chunk will be reserved after clearing,
    * and only 128M can be reused when you apply for 4G memory again.
    */
    void clear(bool delete_head = false) {
        if (head == nullptr) {
            return;
        }

        if (head->prev) {
            delete head->prev;
            head->prev = nullptr;
        }
        if (delete_head) {
            delete head;
            head = nullptr;
            size_in_bytes = 0;
        } else {
            head->pos = head->begin;
            size_in_bytes = head->size();
        }
        _used_size_no_head = 0;
    }

    /// Size of chunks in bytes.
    size_t size() const { return size_in_bytes; }

    size_t used_size() const {
        if (head == nullptr) {
            return _used_size_no_head;
        }

        return _used_size_no_head + head->used();
    }

    size_t remaining_space_in_current_chunk() const {
        if (head == nullptr) {
            return 0;
        }

        return head->remaining();
    }
};

using ArenaPtr = std::shared_ptr<Arena>;
using Arenas = std::vector<ArenaPtr>;

} // namespace doris::vectorized
