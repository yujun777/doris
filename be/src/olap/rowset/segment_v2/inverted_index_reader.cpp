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

#include "olap/rowset/segment_v2/inverted_index_reader.h"

#include <CLucene/debug/error.h>
#include <CLucene/debug/mem.h>
#include <CLucene/index/Term.h>
#include <CLucene/search/Query.h>
#include <CLucene/search/RangeQuery.h>
#include <CLucene/store/Directory.h>
#include <CLucene/store/IndexInput.h>
#include <CLucene/util/FutureArrays.h>
#include <CLucene/util/bkd/bkd_docid_iterator.h>
#include <CLucene/util/stringUtil.h>

#include <memory>
#include <ostream>
#include <roaring/roaring.hh>
#include <set>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "inverted_index_query_type.h"
#include "olap/inverted_index_parser.h"
#include "olap/key_coder.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/index_file_reader.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/query/phrase_query.h"
#include "olap/rowset/segment_v2/inverted_index/query/query_factory.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/rowset/segment_v2/inverted_index_iterator.h"
#include "olap/rowset/segment_v2/inverted_index_searcher.h"
#include "olap/types.h"
#include "runtime/runtime_state.h"
#include "util/faststring.h"
#include "util/runtime_profile.h"
#include "vec/common/string_ref.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

template <PrimitiveType PT>
Status InvertedIndexQueryParamFactory::create_query_value(
        const void* value, std::unique_ptr<InvertedIndexQueryParamFactory>& result_param) {
    using CPP_TYPE = typename PrimitiveTypeTraits<PT>::CppType;
    std::unique_ptr<InvertedIndexQueryParam<PT>> param =
            InvertedIndexQueryParam<PT>::create_unique();
    auto&& storage_val = PrimitiveTypeConvertor<PT>::to_storage_field_type(
            *reinterpret_cast<const CPP_TYPE*>(value));
    param->set_value(&storage_val);
    result_param = std::move(param);
    return Status::OK();
};

#define CREATE_QUERY_VALUE_TEMPLATE(PT)                                     \
    template Status InvertedIndexQueryParamFactory::create_query_value<PT>( \
            const void* value, std::unique_ptr<InvertedIndexQueryParamFactory>& result_param);

CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_BOOLEAN)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_TINYINT)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_SMALLINT)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_INT)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_BIGINT)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_LARGEINT)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_FLOAT)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_DOUBLE)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_VARCHAR)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_DATE)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_DATEV2)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_DATETIME)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_DATETIMEV2)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_CHAR)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_DECIMALV2)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_DECIMAL32)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_DECIMAL64)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_DECIMAL128I)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_DECIMAL256)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_HLL)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_STRING)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_IPV4)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_IPV6)

std::string InvertedIndexReader::get_index_file_path() {
    return _index_file_reader->get_index_file_path(&_index_meta);
}

Status InvertedIndexReader::read_null_bitmap(const io::IOContext* io_ctx,
                                             OlapReaderStatistics* stats,
                                             InvertedIndexQueryCacheHandle* cache_handle,
                                             lucene::store::Directory* dir) {
    SCOPED_RAW_TIMER(&stats->inverted_index_query_null_bitmap_timer);
    lucene::store::IndexInput* null_bitmap_in = nullptr;
    bool owned_dir = false;
    try {
        // try to get query bitmap result from cache and return immediately on cache hit
        auto index_file_key = _index_file_reader->get_index_file_cache_key(&_index_meta);
        InvertedIndexQueryCache::CacheKey cache_key {
                index_file_key, "", InvertedIndexQueryType::UNKNOWN_QUERY, "null_bitmap"};
        auto* cache = InvertedIndexQueryCache::instance();
        if (cache->lookup(cache_key, cache_handle)) {
            return Status::OK();
        }

        if (!dir) {
            // TODO: ugly code here, try to refact.
            auto st = _index_file_reader->init(config::inverted_index_read_buffer_size, io_ctx);
            if (!st.ok()) {
                LOG(WARNING) << st;
                return st;
            }
            auto directory = DORIS_TRY(_index_file_reader->open(&_index_meta, io_ctx));
            dir = directory.release();
            owned_dir = true;
        }

        // ownership of null_bitmap and its deletion will be transfered to cache
        std::shared_ptr<roaring::Roaring> null_bitmap = std::make_shared<roaring::Roaring>();
        const char* null_bitmap_file_name =
                InvertedIndexDescriptor::get_temporary_null_bitmap_file_name();
        if (dir->fileExists(null_bitmap_file_name)) {
            null_bitmap_in = dir->openInput(null_bitmap_file_name);
            auto null_bitmap_size = cast_set<int32_t>(null_bitmap_in->length());
            faststring buf;
            buf.resize(null_bitmap_size);
            null_bitmap_in->readBytes(buf.data(), null_bitmap_size);
            *null_bitmap = roaring::Roaring::read(reinterpret_cast<char*>(buf.data()), false);
            null_bitmap->runOptimize();
            cache->insert(cache_key, null_bitmap, cache_handle);
            FINALIZE_INPUT(null_bitmap_in);
        } else {
            cache->insert(cache_key, null_bitmap, cache_handle);
        }
        if (owned_dir) {
            FINALIZE_INPUT(dir);
        }
    } catch (CLuceneError& e) {
        FINALLY_FINALIZE_INPUT(null_bitmap_in);
        if (owned_dir) {
            FINALLY_FINALIZE_INPUT(dir);
        }
        return Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "Inverted index read null bitmap error occurred, reason={}", e.what());
    }

    return Status::OK();
}

Status InvertedIndexReader::handle_query_cache(RuntimeState* runtime_state,
                                               InvertedIndexQueryCache* cache,
                                               const InvertedIndexQueryCache::CacheKey& cache_key,
                                               InvertedIndexQueryCacheHandle* cache_handler,
                                               OlapReaderStatistics* stats,
                                               std::shared_ptr<roaring::Roaring>& bit_map) {
    const auto& query_options = runtime_state->query_options();
    if (query_options.enable_inverted_index_query_cache &&
        cache->lookup(cache_key, cache_handler)) {
        DBUG_EXECUTE_IF("InvertedIndexReader.handle_query_cache_hit", {
            return Status::Error<ErrorCode::INTERNAL_ERROR>("handle query cache hit");
        });
        stats->inverted_index_query_cache_hit++;
        SCOPED_RAW_TIMER(&stats->inverted_index_query_bitmap_copy_timer);
        bit_map = cache_handler->get_bitmap();
        return Status::OK();
    }
    DBUG_EXECUTE_IF("InvertedIndexReader.handle_query_cache_miss", {
        return Status::Error<ErrorCode::INTERNAL_ERROR>("handle query cache miss");
    });
    stats->inverted_index_query_cache_miss++;
    return Status::Error<ErrorCode::KEY_NOT_FOUND>("cache miss");
}

Status InvertedIndexReader::handle_searcher_cache(
        RuntimeState* runtime_state, InvertedIndexCacheHandle* inverted_index_cache_handle,
        const io::IOContext* io_ctx, OlapReaderStatistics* stats) {
    auto index_file_key = _index_file_reader->get_index_file_cache_key(&_index_meta);
    InvertedIndexSearcherCache::CacheKey searcher_cache_key(index_file_key);
    const auto& query_options = runtime_state->query_options();
    if (query_options.enable_inverted_index_searcher_cache &&
        InvertedIndexSearcherCache::instance()->lookup(searcher_cache_key,
                                                       inverted_index_cache_handle)) {
        DBUG_EXECUTE_IF("InvertedIndexReader.handle_searcher_cache_hit", {
            return Status::Error<ErrorCode::INTERNAL_ERROR>("handle searcher cache hit");
        });
        stats->inverted_index_searcher_cache_hit++;
        return Status::OK();
    } else {
        DBUG_EXECUTE_IF("InvertedIndexReader.handle_searcher_cache_miss", {
            return Status::Error<ErrorCode::INTERNAL_ERROR>("handle searcher cache miss");
        });
        // searcher cache miss
        stats->inverted_index_searcher_cache_miss++;
        auto mem_tracker = std::make_unique<MemTracker>("InvertedIndexSearcherCacheWithRead");
        SCOPED_RAW_TIMER(&stats->inverted_index_searcher_open_timer);
        IndexSearcherPtr searcher;

        auto st = _index_file_reader->init(config::inverted_index_read_buffer_size, io_ctx);
        if (!st.ok()) {
            LOG(WARNING) << st;
            return st;
        }
        auto dir = DORIS_TRY(_index_file_reader->open(&_index_meta, io_ctx));

        DBUG_EXECUTE_IF("InvertedIndexReader.handle_searcher_cache.io_ctx", ({
                            if (dir) {
                                auto* stream = dir->getDorisIndexInput();
                                const auto* cur_io_ctx =
                                        (const io::IOContext*)stream->getIoContext();
                                if (cur_io_ctx->file_cache_stats) {
                                    if (cur_io_ctx->file_cache_stats != &stats->file_cache_stats) {
                                        LOG(FATAL) << "io context file cache stats is not equal to "
                                                      "stats file cache "
                                                      "stats: "
                                                   << cur_io_ctx->file_cache_stats << ", "
                                                   << &stats->file_cache_stats;
                                    }
                                }
                            }
                        }));

        // try to reuse index_searcher's directory to read null_bitmap to cache
        // to avoid open directory additionally for null_bitmap
        // TODO: handle null bitmap procedure in new format.
        InvertedIndexQueryCacheHandle null_bitmap_cache_handle;
        RETURN_IF_ERROR(read_null_bitmap(io_ctx, stats, &null_bitmap_cache_handle, dir.get()));
        size_t reader_size = 0;
        auto index_searcher_builder =
                DORIS_TRY(IndexSearcherBuilder::create_index_searcher_builder(type()));
        RETURN_IF_ERROR(create_index_searcher(index_searcher_builder.get(), dir.release(),
                                              &searcher, reader_size));
        auto* cache_value = new InvertedIndexSearcherCache::CacheValue(std::move(searcher),
                                                                       reader_size, UnixMillis());
        InvertedIndexSearcherCache::instance()->insert(searcher_cache_key, cache_value,
                                                       inverted_index_cache_handle);
        return Status::OK();
    }
}

Status InvertedIndexReader::create_index_searcher(IndexSearcherBuilder* index_searcher_builder,
                                                  lucene::store::Directory* dir,
                                                  IndexSearcherPtr* searcher, size_t& reader_size) {
    auto searcher_result = DORIS_TRY(index_searcher_builder->get_index_searcher(dir));
    *searcher = searcher_result;

    // When the meta information has been read, the ioContext needs to be reset to prevent it from being used by other queries.
    auto* stream = static_cast<DorisCompoundReader*>(dir)->getDorisIndexInput();
    stream->setIoContext(nullptr);
    stream->setIndexFile(false);

    reader_size = index_searcher_builder->get_reader_size();
    return Status::OK();
};

Status InvertedIndexReader::match_index_search(
        const io::IOContext* io_ctx, OlapReaderStatistics* stats, RuntimeState* runtime_state,
        InvertedIndexQueryType query_type, const InvertedIndexQueryInfo& query_info,
        const FulltextIndexSearcherPtr& index_searcher,
        const std::shared_ptr<roaring::Roaring>& term_match_bitmap) {
    const auto& queryOptions = runtime_state->query_options();
    auto* reader = index_searcher->getReader();
    if (runtime_state && runtime_state->query_options().inverted_index_compatible_read) {
        reader->setCompatibleRead(true);
    }
    try {
        SCOPED_RAW_TIMER(&stats->inverted_index_searcher_search_timer);
        auto query = QueryFactory::create(query_type, index_searcher, queryOptions, io_ctx);
        if (!query) {
            return Status::Error<ErrorCode::INDEX_INVALID_PARAMETERS>(
                    "query type " + query_type_to_string(query_type) + ", query is nullptr");
        }
        {
            SCOPED_RAW_TIMER(&stats->inverted_index_searcher_search_init_timer);
            query->add(query_info);
        }
        {
            SCOPED_RAW_TIMER(&stats->inverted_index_searcher_search_exec_timer);
            query->search(*term_match_bitmap);
        }
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>("CLuceneError occured: {}",
                                                                      e.what());
    } catch (const Exception& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>("Exception occured: {}",
                                                                      e.what());
    }
    return Status::OK();
}

bool InvertedIndexReader::is_support_phrase() {
    return get_parser_phrase_support_string_from_properties(get_index_properties()) ==
           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES;
}

Status FullTextIndexReader::new_iterator(const io::IOContext& io_ctx, OlapReaderStatistics* stats,
                                         RuntimeState* runtime_state,
                                         std::unique_ptr<IndexIterator>* iterator) {
    *iterator =
            InvertedIndexIterator::create_unique(io_ctx, stats, runtime_state, shared_from_this());
    return Status::OK();
}

Status FullTextIndexReader::query(const io::IOContext* io_ctx, OlapReaderStatistics* stats,
                                  RuntimeState* runtime_state, const std::string& column_name,
                                  const void* query_value, InvertedIndexQueryType query_type,
                                  std::shared_ptr<roaring::Roaring>& bit_map) {
    SCOPED_RAW_TIMER(&stats->inverted_index_query_timer);

    std::string search_str = reinterpret_cast<const StringRef*>(query_value)->to_string();
    VLOG_DEBUG << column_name << " begin to search the fulltext index from clucene, query_str ["
               << search_str << "]";

    const auto& queryOptions = runtime_state->query_options();
    try {
        InvertedIndexQueryInfo query_info;
        InvertedIndexQueryCache::CacheKey cache_key;
        auto index_file_key = _index_file_reader->get_index_file_cache_key(&_index_meta);

        // terms
        if (query_type == InvertedIndexQueryType::MATCH_REGEXP_QUERY) {
            query_info.term_infos.emplace_back(search_str, 0);
        } else if (query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY) {
            PhraseQuery::parser_info(search_str, _index_meta.properties(), query_info);
        } else {
            query_info.term_infos = inverted_index::InvertedIndexAnalyzer::get_analyse_result(
                    search_str, _index_meta.properties());
        }

        if (query_info.term_infos.empty()) {
            auto msg = fmt::format(
                    "token parser result is empty for query, "
                    "please check your query: '{}' and index parser: '{}'",
                    search_str, get_parser_string_from_properties(_index_meta.properties()));
            if (is_match_query(query_type)) {
                LOG(WARNING) << msg;
                return Status::OK();
            } else {
                return Status::Error<ErrorCode::INVERTED_INDEX_NO_TERMS>(msg);
            }
        }

        // field_name
        query_info.field_name = StringUtil::string_to_wstring(column_name);

        // cache_key
        std::string str_tokens = query_info.generate_tokens_key();
        if (query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY) {
            str_tokens += " " + std::to_string(query_info.slop);
            str_tokens += " " + std::to_string(query_info.ordered);
        } else if (query_type == InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY ||
                   query_type == InvertedIndexQueryType::MATCH_REGEXP_QUERY) {
            str_tokens += " " + std::to_string(queryOptions.inverted_index_max_expansions);
        }
        cache_key = {index_file_key, column_name, query_type, std::move(str_tokens)};

        auto* cache = InvertedIndexQueryCache::instance();
        InvertedIndexQueryCacheHandle cache_handler;

        std::shared_ptr<roaring::Roaring> term_match_bitmap = nullptr;
        auto cache_status =
                handle_query_cache(runtime_state, cache, cache_key, &cache_handler, stats, bit_map);
        if (cache_status.ok()) {
            return Status::OK();
        }

        InvertedIndexCacheHandle inverted_index_cache_handle;
        RETURN_IF_ERROR(
                handle_searcher_cache(runtime_state, &inverted_index_cache_handle, io_ctx, stats));
        auto searcher_variant = inverted_index_cache_handle.get_index_searcher();
        auto* searcher_ptr = std::get_if<FulltextIndexSearcherPtr>(&searcher_variant);
        if (searcher_ptr != nullptr) {
            term_match_bitmap = std::make_shared<roaring::Roaring>();
            RETURN_IF_ERROR(match_index_search(io_ctx, stats, runtime_state, query_type, query_info,
                                               *searcher_ptr, term_match_bitmap));
            term_match_bitmap->runOptimize();
            cache->insert(cache_key, term_match_bitmap, &cache_handler);
            bit_map = term_match_bitmap;
        }
        return Status::OK();
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "CLuceneError occured, error msg: {}", e.what());
    }
}

InvertedIndexReaderType FullTextIndexReader::type() {
    return InvertedIndexReaderType::FULLTEXT;
}

Status StringTypeInvertedIndexReader::new_iterator(const io::IOContext& io_ctx,
                                                   OlapReaderStatistics* stats,
                                                   RuntimeState* runtime_state,
                                                   std::unique_ptr<IndexIterator>* iterator) {
    *iterator =
            InvertedIndexIterator::create_unique(io_ctx, stats, runtime_state, shared_from_this());
    return Status::OK();
}

Status StringTypeInvertedIndexReader::query(const io::IOContext* io_ctx,
                                            OlapReaderStatistics* stats,
                                            RuntimeState* runtime_state,
                                            const std::string& column_name, const void* query_value,
                                            InvertedIndexQueryType query_type,
                                            std::shared_ptr<roaring::Roaring>& bit_map) {
    SCOPED_RAW_TIMER(&stats->inverted_index_query_timer);

    const auto* search_query = reinterpret_cast<const StringRef*>(query_value);
    auto act_len = strnlen(search_query->data, search_query->size);

    // If the written value exceeds ignore_above, it will be written as null.
    // The queried value exceeds ignore_above means the written value cannot be found.
    // The query needs to be downgraded to read from the segment file.
    if (int ignore_above =
                std::stoi(get_parser_ignore_above_value_from_properties(_index_meta.properties()));
        act_len > ignore_above) {
        return Status::Error<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>(
                "query value is too long, evaluate skipped.");
    }

    std::string search_str(search_query->data, act_len);
    VLOG_DEBUG << "begin to query the inverted index from clucene"
               << ", column_name: " << column_name << ", search_str: " << search_str;
    try {
        auto index_file_key = _index_file_reader->get_index_file_cache_key(&_index_meta);
        // try to get query bitmap result from cache and return immediately on cache hit
        InvertedIndexQueryCache::CacheKey cache_key {index_file_key, column_name, query_type,
                                                     search_str};
        auto* cache = InvertedIndexQueryCache::instance();
        InvertedIndexQueryCacheHandle cache_handler;
        auto cache_status =
                handle_query_cache(runtime_state, cache, cache_key, &cache_handler, stats, bit_map);
        if (cache_status.ok()) {
            return Status::OK();
        }

        std::wstring column_name_ws = StringUtil::string_to_wstring(column_name);

        InvertedIndexQueryInfo query_info;
        query_info.field_name = column_name_ws;
        query_info.term_infos.emplace_back(search_str, 0);

        auto result = std::make_shared<roaring::Roaring>();
        FulltextIndexSearcherPtr* searcher_ptr = nullptr;
        InvertedIndexCacheHandle inverted_index_cache_handle;
        RETURN_IF_ERROR(
                handle_searcher_cache(runtime_state, &inverted_index_cache_handle, io_ctx, stats));
        auto searcher_variant = inverted_index_cache_handle.get_index_searcher();
        searcher_ptr = std::get_if<FulltextIndexSearcherPtr>(&searcher_variant);
        if (searcher_ptr != nullptr) {
            switch (query_type) {
            case InvertedIndexQueryType::MATCH_ANY_QUERY:
            case InvertedIndexQueryType::MATCH_ALL_QUERY:
            case InvertedIndexQueryType::EQUAL_QUERY: {
                RETURN_IF_ERROR(match_index_search(io_ctx, stats, runtime_state,
                                                   InvertedIndexQueryType::MATCH_ANY_QUERY,
                                                   query_info, *searcher_ptr, result));
                break;
            }
            case InvertedIndexQueryType::MATCH_PHRASE_QUERY:
            case InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY:
            case InvertedIndexQueryType::MATCH_REGEXP_QUERY: {
                RETURN_IF_ERROR(match_index_search(io_ctx, stats, runtime_state, query_type,
                                                   query_info, *searcher_ptr, result));
                break;
            }
            case InvertedIndexQueryType::LESS_THAN_QUERY:
            case InvertedIndexQueryType::LESS_EQUAL_QUERY:
            case InvertedIndexQueryType::GREATER_THAN_QUERY:
            case InvertedIndexQueryType::GREATER_EQUAL_QUERY: {
                std::wstring search_str_ws = StringUtil::string_to_wstring(search_str);
                // unique_ptr with custom deleter
                std::unique_ptr<lucene::index::Term, void (*)(lucene::index::Term*)> term {
                        _CLNEW lucene::index::Term(column_name_ws.c_str(), search_str_ws.c_str()),
                        [](lucene::index::Term* term) { _CLDECDELETE(term); }};
                std::unique_ptr<lucene::search::Query> query;

                bool include_upper = query_type == InvertedIndexQueryType::LESS_EQUAL_QUERY;
                bool include_lower = query_type == InvertedIndexQueryType::GREATER_EQUAL_QUERY;

                if (query_type == InvertedIndexQueryType::LESS_THAN_QUERY ||
                    query_type == InvertedIndexQueryType::LESS_EQUAL_QUERY) {
                    query = std::make_unique<lucene::search::RangeQuery>(nullptr, term.get(),
                                                                         include_upper);
                } else { // GREATER_THAN_QUERY or GREATER_EQUAL_QUERY
                    query = std::make_unique<lucene::search::RangeQuery>(term.get(), nullptr,
                                                                         include_lower);
                }

                SCOPED_RAW_TIMER(&stats->inverted_index_searcher_search_timer);
                (*searcher_ptr)
                        ->_search(query.get(),
                                  [&result](const int32_t docid, const float_t /*score*/) {
                                      result->add(docid);
                                  });
                break;
            }
            default:
                return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                        "invalid query type when query untokenized inverted index");
            }
        }
        // add to cache
        result->runOptimize();
        cache->insert(cache_key, result, &cache_handler);

        bit_map = result;
        return Status::OK();
    } catch (const CLuceneError& e) {
        if (is_range_query(query_type) && e.number() == CL_ERR_TooManyClauses) {
            return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                    "range query term exceeds limits, try to downgrade from inverted index, "
                    "column "
                    "name:{}, search_str:{}",
                    column_name, search_str);
        } else {
            LOG(ERROR) << "CLuceneError occurred, error msg: " << e.what()
                       << ", column name: " << column_name << ", search_str: " << search_str;
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "CLuceneError occurred, error msg: {}, column name: {}, search_str: {}",
                    e.what(), column_name, search_str);
        }
    }
}

InvertedIndexReaderType StringTypeInvertedIndexReader::type() {
    return InvertedIndexReaderType::STRING_TYPE;
}

Status BkdIndexReader::new_iterator(const io::IOContext& io_ctx, OlapReaderStatistics* stats,
                                    RuntimeState* runtime_state,
                                    std::unique_ptr<IndexIterator>* iterator) {
    *iterator =
            InvertedIndexIterator::create_unique(io_ctx, stats, runtime_state, shared_from_this());
    return Status::OK();
}

template <InvertedIndexQueryType QT>
Status BkdIndexReader::construct_bkd_query_value(const void* query_value,
                                                 std::shared_ptr<lucene::util::bkd::bkd_reader> r,
                                                 InvertedIndexVisitor<QT>* visitor) {
    std::vector<char> tmp(r->bytes_per_dim_);
    if constexpr (QT == InvertedIndexQueryType::EQUAL_QUERY) {
        _value_key_coder->full_encode_ascending(query_value, &visitor->query_max);
        _value_key_coder->full_encode_ascending(query_value, &visitor->query_min);
    } else if constexpr (QT == InvertedIndexQueryType::LESS_THAN_QUERY ||
                         QT == InvertedIndexQueryType::LESS_EQUAL_QUERY) {
        _value_key_coder->full_encode_ascending(query_value, &visitor->query_max);
        _type_info->set_to_min(tmp.data());
        _value_key_coder->full_encode_ascending(tmp.data(), &visitor->query_min);
    } else if constexpr (QT == InvertedIndexQueryType::GREATER_THAN_QUERY ||
                         QT == InvertedIndexQueryType::GREATER_EQUAL_QUERY) {
        _value_key_coder->full_encode_ascending(query_value, &visitor->query_min);
        _type_info->set_to_max(tmp.data());
        _value_key_coder->full_encode_ascending(tmp.data(), &visitor->query_max);
    } else {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "invalid query type when query bkd index");
    }
    return Status::OK();
}

Status BkdIndexReader::invoke_bkd_try_query(const io::IOContext* io_ctx, const void* query_value,
                                            InvertedIndexQueryType query_type,
                                            std::shared_ptr<lucene::util::bkd::bkd_reader> r,
                                            size_t* count) {
    switch (query_type) {
    case InvertedIndexQueryType::LESS_THAN_QUERY: {
        auto visitor =
                std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::LESS_THAN_QUERY>>(
                        io_ctx, r.get(), nullptr, true);
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        *count = r->estimate_point_count(visitor.get());
        break;
    }
    case InvertedIndexQueryType::LESS_EQUAL_QUERY: {
        auto visitor =
                std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::LESS_EQUAL_QUERY>>(
                        io_ctx, r.get(), nullptr, true);
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        *count = r->estimate_point_count(visitor.get());
        break;
    }
    case InvertedIndexQueryType::GREATER_THAN_QUERY: {
        auto visitor =
                std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::GREATER_THAN_QUERY>>(
                        io_ctx, r.get(), nullptr, true);
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        *count = r->estimate_point_count(visitor.get());
        break;
    }
    case InvertedIndexQueryType::GREATER_EQUAL_QUERY: {
        auto visitor =
                std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::GREATER_EQUAL_QUERY>>(
                        io_ctx, r.get(), nullptr, true);
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        *count = r->estimate_point_count(visitor.get());
        break;
    }
    case InvertedIndexQueryType::EQUAL_QUERY: {
        auto visitor = std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::EQUAL_QUERY>>(
                io_ctx, r.get(), nullptr, true);
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        *count = r->estimate_point_count(visitor.get());
        break;
    }
    default:
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>("Invalid query type");
    }
    return Status::OK();
}

Status BkdIndexReader::invoke_bkd_query(const io::IOContext* io_ctx, OlapReaderStatistics* stats,
                                        const void* query_value, InvertedIndexQueryType query_type,
                                        std::shared_ptr<lucene::util::bkd::bkd_reader> r,
                                        std::shared_ptr<roaring::Roaring>& bit_map) {
    SCOPED_RAW_TIMER(&stats->inverted_index_searcher_search_timer);
    switch (query_type) {
    case InvertedIndexQueryType::LESS_THAN_QUERY: {
        auto visitor =
                std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::LESS_THAN_QUERY>>(
                        io_ctx, r.get(), bit_map.get());
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        r->intersect(visitor.get());
        break;
    }
    case InvertedIndexQueryType::LESS_EQUAL_QUERY: {
        auto visitor =
                std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::LESS_EQUAL_QUERY>>(
                        io_ctx, r.get(), bit_map.get());
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        r->intersect(visitor.get());
        break;
    }
    case InvertedIndexQueryType::GREATER_THAN_QUERY: {
        auto visitor =
                std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::GREATER_THAN_QUERY>>(
                        io_ctx, r.get(), bit_map.get());
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        r->intersect(visitor.get());
        break;
    }
    case InvertedIndexQueryType::GREATER_EQUAL_QUERY: {
        auto visitor =
                std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::GREATER_EQUAL_QUERY>>(
                        io_ctx, r.get(), bit_map.get());
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        r->intersect(visitor.get());
        break;
    }
    case InvertedIndexQueryType::EQUAL_QUERY: {
        auto visitor = std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::EQUAL_QUERY>>(
                io_ctx, r.get(), bit_map.get());
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        r->intersect(visitor.get());
        break;
    }
    default:
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>("Invalid query type");
    }
    return Status::OK();
}

Status BkdIndexReader::try_query(const io::IOContext* io_ctx, OlapReaderStatistics* stats,
                                 RuntimeState* runtime_state, const std::string& column_name,
                                 const void* query_value, InvertedIndexQueryType query_type,
                                 size_t* count) {
    try {
        std::shared_ptr<lucene::util::bkd::bkd_reader> r;
        auto st = get_bkd_reader(r, io_ctx, stats, runtime_state);
        if (!st.ok()) {
            LOG(WARNING) << "get bkd reader for  "
                         << _index_file_reader->get_index_file_path(&_index_meta)
                         << " failed: " << st;
            return st;
        }
        std::string query_str;
        _value_key_coder->full_encode_ascending(query_value, &query_str);

        auto index_file_key = _index_file_reader->get_index_file_cache_key(&_index_meta);
        InvertedIndexQueryCache::CacheKey cache_key {index_file_key, column_name, query_type,
                                                     query_str};
        auto* cache = InvertedIndexQueryCache::instance();
        InvertedIndexQueryCacheHandle cache_handler;
        std::shared_ptr<roaring::Roaring> bit_map;
        auto cache_status =
                handle_query_cache(runtime_state, cache, cache_key, &cache_handler, stats, bit_map);
        if (cache_status.ok()) {
            *count = bit_map->cardinality();
            return Status::OK();
        }

        return invoke_bkd_try_query(io_ctx, query_value, query_type, r, count);
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "BKD Query CLuceneError Occurred, error msg: {}", e.what());
    }

    VLOG_DEBUG << "BKD index try search column: " << column_name << " result: " << *count;
    return Status::OK();
}

Status BkdIndexReader::query(const io::IOContext* io_ctx, OlapReaderStatistics* stats,
                             RuntimeState* runtime_state, const std::string& column_name,
                             const void* query_value, InvertedIndexQueryType query_type,
                             std::shared_ptr<roaring::Roaring>& bit_map) {
    SCOPED_RAW_TIMER(&stats->inverted_index_query_timer);

    try {
        std::shared_ptr<lucene::util::bkd::bkd_reader> r;
        auto st = get_bkd_reader(r, io_ctx, stats, runtime_state);
        if (!st.ok()) {
            LOG(WARNING) << "get bkd reader for  "
                         << _index_file_reader->get_index_file_path(&_index_meta)
                         << " failed: " << st;
            return st;
        }
        std::string query_str;
        _value_key_coder->full_encode_ascending(query_value, &query_str);

        auto index_file_key = _index_file_reader->get_index_file_cache_key(&_index_meta);
        InvertedIndexQueryCache::CacheKey cache_key {index_file_key, column_name, query_type,
                                                     query_str};
        auto* cache = InvertedIndexQueryCache::instance();
        InvertedIndexQueryCacheHandle cache_handler;
        auto cache_status =
                handle_query_cache(runtime_state, cache, cache_key, &cache_handler, stats, bit_map);
        if (cache_status.ok()) {
            return Status::OK();
        }

        RETURN_IF_ERROR(invoke_bkd_query(io_ctx, stats, query_value, query_type, r, bit_map));
        bit_map->runOptimize();
        cache->insert(cache_key, bit_map, &cache_handler);

        VLOG_DEBUG << "BKD index search column: " << column_name
                   << " result: " << bit_map->cardinality();

        return Status::OK();
    } catch (const CLuceneError& e) {
        LOG(ERROR) << "BKD Query CLuceneError Occurred, error msg:  " << e.what()
                   << " file_path:" << _index_file_reader->get_index_file_path(&_index_meta);
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "BKD Query CLuceneError Occurred, error msg: {}", e.what());
    }
}

Status BkdIndexReader::get_bkd_reader(BKDIndexSearcherPtr& bkd_reader, const io::IOContext* io_ctx,
                                      OlapReaderStatistics* stats, RuntimeState* runtime_state) {
    BKDIndexSearcherPtr* bkd_searcher = nullptr;
    InvertedIndexCacheHandle inverted_index_cache_handle;
    RETURN_IF_ERROR(
            handle_searcher_cache(runtime_state, &inverted_index_cache_handle, io_ctx, stats));
    auto searcher_variant = inverted_index_cache_handle.get_index_searcher();
    bkd_searcher = std::get_if<BKDIndexSearcherPtr>(&searcher_variant);
    if (bkd_searcher) {
        _type_info = get_scalar_type_info((FieldType)(*bkd_searcher)->type);
        if (_type_info == nullptr) {
            return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                    "unsupported typeinfo, type={}", (*bkd_searcher)->type);
        }
        _value_key_coder = get_key_coder(_type_info->type());
        bkd_reader = *bkd_searcher;
        if (bkd_reader->bytes_per_dim_ == 0) {
            bkd_reader->bytes_per_dim_ = cast_set<int32_t>(_type_info->size());
        }
        return Status::OK();
    }
    return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
            "get bkd reader from searcher cache builder error");
}

InvertedIndexReaderType BkdIndexReader::type() {
    return InvertedIndexReaderType::BKD;
}

template <InvertedIndexQueryType QT>
InvertedIndexVisitor<QT>::InvertedIndexVisitor(const void* io_ctx, lucene::util::bkd::bkd_reader* r,
                                               roaring::Roaring* h, bool only_count)
        : _io_ctx(io_ctx), _hits(h), _num_hits(0), _only_count(only_count), _reader(r) {}

template <InvertedIndexQueryType QT>
int InvertedIndexVisitor<QT>::matches(uint8_t* packed_value) {
    if (UNLIKELY(_reader == nullptr)) {
        throw CLuceneError(CL_ERR_NullPointer, "bkd index reader is null", false);
    }
    bool all_greater_than_max = true;
    bool all_within_range = true;

    for (int dim = 0; dim < _reader->num_data_dims_; dim++) {
        int offset = dim * _reader->bytes_per_dim_;

        auto result_max = lucene::util::FutureArrays::CompareUnsigned(
                packed_value, offset, offset + _reader->bytes_per_dim_,
                (const uint8_t*)query_max.c_str(), offset, offset + _reader->bytes_per_dim_);

        auto result_min = lucene::util::FutureArrays::CompareUnsigned(
                packed_value, offset, offset + _reader->bytes_per_dim_,
                (const uint8_t*)query_min.c_str(), offset, offset + _reader->bytes_per_dim_);

        all_greater_than_max &= (result_max > 0);
        all_within_range &= (result_min > 0 && result_max < 0);

        if (!all_greater_than_max && !all_within_range) {
            return -1;
        }
    }

    if (all_greater_than_max) {
        return 1;
    } else if (all_within_range) {
        return 0;
    } else {
        return -1;
    }
}

template <>
int InvertedIndexVisitor<InvertedIndexQueryType::EQUAL_QUERY>::matches(uint8_t* packed_value) {
    if (UNLIKELY(_reader == nullptr)) {
        throw CLuceneError(CL_ERR_NullPointer, "bkd index reader is null", false);
    }
    // if query type is equal, query_min == query_max
    if (_reader->num_data_dims_ == 1) {
        return std::memcmp(packed_value, (const uint8_t*)query_min.c_str(),
                           _reader->bytes_per_dim_);
    } else {
        // if all dim value > matched value, then return > 0, otherwise return < 0
        int return_result = 0;
        for (int dim = 0; dim < _reader->num_data_dims_; dim++) {
            int offset = dim * _reader->bytes_per_dim_;
            auto result = lucene::util::FutureArrays::CompareUnsigned(
                    packed_value, offset, offset + _reader->bytes_per_dim_,
                    (const uint8_t*)query_min.c_str(), offset, offset + _reader->bytes_per_dim_);
            if (result < 0) {
                return -1;
            } else if (result > 0) {
                return_result = 1;
            }
        }
        return return_result;
    }
}

template <>
int InvertedIndexVisitor<InvertedIndexQueryType::LESS_THAN_QUERY>::matches(uint8_t* packed_value) {
    if (UNLIKELY(_reader == nullptr)) {
        throw CLuceneError(CL_ERR_NullPointer, "bkd index reader is null", false);
    }
    if (_reader->num_data_dims_ == 1) {
        auto result = std::memcmp(packed_value, (const uint8_t*)query_max.c_str(),
                                  _reader->bytes_per_dim_);
        if (result >= 0) {
            return 1;
        }
        return 0;
    } else {
        bool all_greater_or_equal = true;
        bool all_lesser = true;

        for (int dim = 0; dim < _reader->num_data_dims_; dim++) {
            int offset = dim * _reader->bytes_per_dim_;
            auto result = lucene::util::FutureArrays::CompareUnsigned(
                    packed_value, offset, offset + _reader->bytes_per_dim_,
                    (const uint8_t*)query_max.c_str(), offset, offset + _reader->bytes_per_dim_);

            all_greater_or_equal &=
                    (result >= 0);      // Remains true only if all results are greater or equal
            all_lesser &= (result < 0); // Remains true only if all results are lesser
        }

        // Return 1 if all values are greater or equal, 0 if all are lesser, otherwise -1
        return all_greater_or_equal ? 1 : (all_lesser ? 0 : -1);
    }
}

template <>
int InvertedIndexVisitor<InvertedIndexQueryType::LESS_EQUAL_QUERY>::matches(uint8_t* packed_value) {
    if (UNLIKELY(_reader == nullptr)) {
        throw CLuceneError(CL_ERR_NullPointer, "bkd index reader is null", false);
    }
    if (_reader->num_data_dims_ == 1) {
        auto result = std::memcmp(packed_value, (const uint8_t*)query_max.c_str(),
                                  _reader->bytes_per_dim_);
        if (result > 0) {
            return 1;
        }
        return 0;
    } else {
        bool all_greater = true;
        bool all_lesser_or_equal = true;

        for (int dim = 0; dim < _reader->num_data_dims_; dim++) {
            int offset = dim * _reader->bytes_per_dim_;
            auto result = lucene::util::FutureArrays::CompareUnsigned(
                    packed_value, offset, offset + _reader->bytes_per_dim_,
                    (const uint8_t*)query_max.c_str(), offset, offset + _reader->bytes_per_dim_);

            all_greater &= (result > 0); // Remains true only if all results are greater
            all_lesser_or_equal &=
                    (result <= 0); // Remains true only if all results are lesser or equal
        }

        // Return 1 if all values are greater or equal, 0 if all are lesser, otherwise -1
        return all_greater ? 1 : (all_lesser_or_equal ? 0 : -1);
    }
}

template <>
int InvertedIndexVisitor<InvertedIndexQueryType::GREATER_THAN_QUERY>::matches(
        uint8_t* packed_value) {
    if (UNLIKELY(_reader == nullptr)) {
        throw CLuceneError(CL_ERR_NullPointer, "bkd index reader is null", false);
    }
    if (_reader->num_data_dims_ == 1) {
        auto result = std::memcmp(packed_value, (const uint8_t*)query_min.c_str(),
                                  _reader->bytes_per_dim_);
        if (result <= 0) {
            return -1;
        }
        return 0;
    } else {
        for (int dim = 0; dim < _reader->num_data_dims_; dim++) {
            int offset = dim * _reader->bytes_per_dim_;
            auto result = lucene::util::FutureArrays::CompareUnsigned(
                    packed_value, offset, offset + _reader->bytes_per_dim_,
                    (const uint8_t*)query_min.c_str(), offset, offset + _reader->bytes_per_dim_);
            if (result <= 0) {
                return -1;
            }
        }
        return 0;
    }
}

template <>
int InvertedIndexVisitor<InvertedIndexQueryType::GREATER_EQUAL_QUERY>::matches(
        uint8_t* packed_value) {
    if (UNLIKELY(_reader == nullptr)) {
        throw CLuceneError(CL_ERR_NullPointer, "bkd index reader is null", false);
    }
    if (_reader->num_data_dims_ == 1) {
        auto result = std::memcmp(packed_value, (const uint8_t*)query_min.c_str(),
                                  _reader->bytes_per_dim_);
        if (result < 0) {
            return -1;
        }
        return 0;
    } else {
        for (int dim = 0; dim < _reader->num_data_dims_; dim++) {
            int offset = dim * _reader->bytes_per_dim_;
            auto result = lucene::util::FutureArrays::CompareUnsigned(
                    packed_value, offset, offset + _reader->bytes_per_dim_,
                    (const uint8_t*)query_min.c_str(), offset, offset + _reader->bytes_per_dim_);
            if (result < 0) {
                return -1;
            }
        }
        return 0;
    }
}

template <InvertedIndexQueryType QT>
void InvertedIndexVisitor<QT>::visit(std::vector<char>& doc_id,
                                     std::vector<uint8_t>& packed_value) {
    if (matches(packed_value.data()) != 0) {
        return;
    }
    visit(roaring::Roaring::read(doc_id.data(), false));
}

template <InvertedIndexQueryType QT>
void InvertedIndexVisitor<QT>::visit(roaring::Roaring* doc_id, std::vector<uint8_t>& packed_value) {
    if (matches(packed_value.data()) != 0) {
        return;
    }
    visit(*doc_id);
}

template <InvertedIndexQueryType QT>
void InvertedIndexVisitor<QT>::visit(roaring::Roaring&& r) {
    if (_only_count) {
        _num_hits += r.cardinality();
    } else {
        *_hits |= r;
    }
}

template <InvertedIndexQueryType QT>
void InvertedIndexVisitor<QT>::visit(roaring::Roaring& r) {
    if (_only_count) {
        _num_hits += r.cardinality();
    } else {
        *_hits |= r;
    }
}

template <InvertedIndexQueryType QT>
void InvertedIndexVisitor<QT>::visit(int row_id) {
    if (_only_count) {
        _num_hits++;
    } else {
        _hits->add(row_id);
    }
}

template <InvertedIndexQueryType QT>
void InvertedIndexVisitor<QT>::visit(lucene::util::bkd::bkd_docid_set_iterator* iter,
                                     std::vector<uint8_t>& packed_value) {
    if (matches(packed_value.data()) != 0) {
        return;
    }
    int32_t doc_id = iter->docid_set->nextDoc();
    while (doc_id != lucene::util::bkd::bkd_docid_set::NO_MORE_DOCS) {
        if (_only_count) {
            _num_hits++;
        } else {
            _hits->add(doc_id);
        }
        doc_id = iter->docid_set->nextDoc();
    }
}

template <InvertedIndexQueryType QT>
int InvertedIndexVisitor<QT>::visit(int row_id, std::vector<uint8_t>& packed_value) {
    auto result = matches(packed_value.data());
    if (result != 0) {
        return result;
    }
    if (_only_count) {
        _num_hits++;
    } else {
        _hits->add(row_id);
    }
    return 0;
}

template <>
lucene::util::bkd::relation InvertedIndexVisitor<InvertedIndexQueryType::LESS_THAN_QUERY>::compare(
        std::vector<uint8_t>& min_packed, std::vector<uint8_t>& max_packed) {
    if (UNLIKELY(_reader == nullptr)) {
        throw CLuceneError(CL_ERR_NullPointer, "bkd index reader is null", false);
    }
    bool crosses = false;
    for (int dim = 0; dim < _reader->num_data_dims_; dim++) {
        int offset = dim * _reader->bytes_per_dim_;
        if (lucene::util::FutureArrays::CompareUnsigned(min_packed.data(), offset,
                                                        offset + _reader->bytes_per_dim_,
                                                        (const uint8_t*)query_max.c_str(), offset,
                                                        offset + _reader->bytes_per_dim_) >= 0) {
            return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
        }
        crosses |= lucene::util::FutureArrays::CompareUnsigned(
                           min_packed.data(), offset, offset + _reader->bytes_per_dim_,
                           (const uint8_t*)query_min.c_str(), offset,
                           offset + _reader->bytes_per_dim_) <= 0 ||
                   lucene::util::FutureArrays::CompareUnsigned(
                           max_packed.data(), offset, offset + _reader->bytes_per_dim_,
                           (const uint8_t*)query_max.c_str(), offset,
                           offset + _reader->bytes_per_dim_) >= 0;
    }
    if (crosses) {
        return lucene::util::bkd::relation::CELL_CROSSES_QUERY;
    } else {
        return lucene::util::bkd::relation::CELL_INSIDE_QUERY;
    }
}

template <>
lucene::util::bkd::relation
InvertedIndexVisitor<InvertedIndexQueryType::GREATER_THAN_QUERY>::compare(
        std::vector<uint8_t>& min_packed, std::vector<uint8_t>& max_packed) {
    if (UNLIKELY(_reader == nullptr)) {
        throw CLuceneError(CL_ERR_NullPointer, "bkd index reader is null", false);
    }
    bool crosses = false;
    for (int dim = 0; dim < _reader->num_data_dims_; dim++) {
        int offset = dim * _reader->bytes_per_dim_;
        if (lucene::util::FutureArrays::CompareUnsigned(max_packed.data(), offset,
                                                        offset + _reader->bytes_per_dim_,
                                                        (const uint8_t*)query_min.c_str(), offset,
                                                        offset + _reader->bytes_per_dim_) <= 0) {
            return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
        }
        crosses |= lucene::util::FutureArrays::CompareUnsigned(
                           min_packed.data(), offset, offset + _reader->bytes_per_dim_,
                           (const uint8_t*)query_min.c_str(), offset,
                           offset + _reader->bytes_per_dim_) <= 0 ||
                   lucene::util::FutureArrays::CompareUnsigned(
                           max_packed.data(), offset, offset + _reader->bytes_per_dim_,
                           (const uint8_t*)query_max.c_str(), offset,
                           offset + _reader->bytes_per_dim_) >= 0;
    }
    if (crosses) {
        return lucene::util::bkd::relation::CELL_CROSSES_QUERY;
    } else {
        return lucene::util::bkd::relation::CELL_INSIDE_QUERY;
    }
}

template <InvertedIndexQueryType QT>
bkd::relation InvertedIndexVisitor<QT>::compare_prefix(std::vector<uint8_t>& prefix) {
    const int32_t length = cast_set<int32_t>(prefix.size());
    const uint8_t* data = prefix.data();

    auto cmp = [&](const std::string& bound) {
        return lucene::util::FutureArrays::CompareUnsigned(
                data, 0, length, reinterpret_cast<const uint8_t*>(bound.data()), 0, length);
    };

    int32_t cmpMax = cmp(query_max);
    int32_t cmpMin = cmp(query_min);

    if (cmpMax > 0 || cmpMin < 0) {
        return bkd::relation::CELL_OUTSIDE_QUERY;
    }
    if (cmpMin > 0 && cmpMax < 0) {
        return bkd::relation::CELL_INSIDE_QUERY;
    }
    return bkd::relation::CELL_CROSSES_QUERY;
}

template <InvertedIndexQueryType QT>
lucene::util::bkd::relation InvertedIndexVisitor<QT>::compare(std::vector<uint8_t>& min_packed,
                                                              std::vector<uint8_t>& max_packed) {
    if (UNLIKELY(_reader == nullptr)) {
        throw CLuceneError(CL_ERR_NullPointer, "bkd index reader is null", false);
    }
    bool crosses = false;
    for (int dim = 0; dim < _reader->num_data_dims_; dim++) {
        int offset = dim * _reader->bytes_per_dim_;
        if (lucene::util::FutureArrays::CompareUnsigned(min_packed.data(), offset,
                                                        offset + _reader->bytes_per_dim_,
                                                        (const uint8_t*)query_max.c_str(), offset,
                                                        offset + _reader->bytes_per_dim_) > 0 ||
            lucene::util::FutureArrays::CompareUnsigned(max_packed.data(), offset,
                                                        offset + _reader->bytes_per_dim_,
                                                        (const uint8_t*)query_min.c_str(), offset,
                                                        offset + _reader->bytes_per_dim_) < 0) {
            return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
        }
        crosses |= lucene::util::FutureArrays::CompareUnsigned(
                           min_packed.data(), offset, offset + _reader->bytes_per_dim_,
                           (const uint8_t*)query_min.c_str(), offset,
                           offset + _reader->bytes_per_dim_) < 0 ||
                   lucene::util::FutureArrays::CompareUnsigned(
                           max_packed.data(), offset, offset + _reader->bytes_per_dim_,
                           (const uint8_t*)query_max.c_str(), offset,
                           offset + _reader->bytes_per_dim_) > 0;
    }
    if (crosses) {
        return lucene::util::bkd::relation::CELL_CROSSES_QUERY;
    } else {
        return lucene::util::bkd::relation::CELL_INSIDE_QUERY;
    }
}

template class InvertedIndexVisitor<InvertedIndexQueryType::LESS_THAN_QUERY>;
template class InvertedIndexVisitor<InvertedIndexQueryType::EQUAL_QUERY>;
template class InvertedIndexVisitor<InvertedIndexQueryType::LESS_EQUAL_QUERY>;
template class InvertedIndexVisitor<InvertedIndexQueryType::GREATER_THAN_QUERY>;
template class InvertedIndexVisitor<InvertedIndexQueryType::GREATER_EQUAL_QUERY>;

} // namespace doris::segment_v2
#include "common/compile_check_end.h"