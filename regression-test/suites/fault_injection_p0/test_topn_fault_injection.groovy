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

suite("test_topn_fault_injection", "nonConcurrent") {
    // define a sql table
    def indexTbName1 = "test_topn_fault_injection1"
    def indexTbName2 = "test_topn_fault_injection2"
    def indexTbName3 = "test_topn_fault_injection3"

    sql "DROP TABLE IF EXISTS ${indexTbName1}"
    sql """
      CREATE TABLE ${indexTbName1} (
        `@timestamp` int(11) NULL COMMENT "",
        `clientip` varchar(20) NULL COMMENT "",
        `request` text NULL COMMENT "",
        `status` int(11) NULL COMMENT "",
        `size` int(11) NULL COMMENT "",
        INDEX clientip_idx (`clientip`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT '',
        INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT ''
      ) ENGINE=OLAP
      DUPLICATE KEY(`@timestamp`)
      COMMENT "OLAP"
      DISTRIBUTED BY RANDOM BUCKETS 1
      PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "disable_auto_compaction" = "true"
      );
    """

    sql "DROP TABLE IF EXISTS ${indexTbName2}"
    sql """
      CREATE TABLE ${indexTbName2} (
        `@timestamp` int(11) NULL COMMENT "",
        `clientip` varchar(20) NULL COMMENT "",
        `request` text NULL COMMENT "",
        `status` int(11) NULL COMMENT "",
        `size` int(11) NULL COMMENT "",
        INDEX clientip_idx (`clientip`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT '',
        INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT ''
      ) ENGINE=OLAP
      UNIQUE KEY(`@timestamp`)
      COMMENT "OLAP"
      DISTRIBUTED BY HASH(`@timestamp`) BUCKETS 1
      PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "enable_unique_key_merge_on_write" = "true",
        "disable_auto_compaction" = "true"
      );
    """

    sql "DROP TABLE IF EXISTS ${indexTbName3}"
    sql """
      CREATE TABLE ${indexTbName3} (
        `@timestamp` int(11) NULL COMMENT "",
        `clientip` varchar(20) NULL COMMENT "",
        `request` text NULL COMMENT "",
        `status` int(11) NULL COMMENT "",
        `size` int(11) NULL COMMENT ""
      ) ENGINE=OLAP
      DUPLICATE KEY(`@timestamp`, `clientip`)
      COMMENT "OLAP"
      DISTRIBUTED BY RANDOM BUCKETS 1
      PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "disable_auto_compaction" = "true"
      );
    """

    def load_httplogs_data = {table_name, label, read_flag, format_flag, file_name, ignore_failure=false,
                        expected_succ_rows = -1, load_to_single_tablet = 'true' ->

        // load the json data
        streamLoad {
            table "${table_name}"

            // set http request header params
            set 'label', label + "_" + UUID.randomUUID().toString()
            set 'read_json_by_line', read_flag
            set 'format', format_flag
            file file_name // import json file
            time 10000 // limit inflight 10s
            if (expected_succ_rows >= 0) {
                set 'max_filter_ratio', '1'
            }

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
		        if (ignore_failure && expected_succ_rows < 0) { return }
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
            }
        }
    }

    try {
      load_httplogs_data.call(indexTbName1, 'test_topn_fault_injection1', 'true', 'json', 'documents-1000.json')
      load_httplogs_data.call(indexTbName2, 'test_topn_fault_injection2', 'true', 'json', 'documents-1000.json')
      load_httplogs_data.call(indexTbName3, 'test_topn_fault_injection3', 'true', 'json', 'documents-1000.json')

      sql "sync"
      sql """ set enable_common_expr_pushdown = true """

      try {
        GetDebugPoint().enableDebugPointForAllBEs("segment_iterator.topn_opt_1")

        qt_sql """ select * from ${indexTbName1} where (request match_phrase 'hm') order by `@timestamp` limit 1; """
        qt_sql """ select * from ${indexTbName1} where (request match_phrase 'hm' and clientip match_phrase '1') order by `@timestamp` limit 1; """
        qt_sql """ select * from ${indexTbName1} where (request match_phrase 'hm' and clientip match_phrase '1') or (request match_phrase 'bg' and clientip match_phrase '2') order by `@timestamp` limit 1; """
        qt_sql """ select * from ${indexTbName1} where (request match_phrase 'hm' and clientip match_phrase '1' or clientip match_phrase '3') or (request match_phrase 'bg' and clientip match_phrase '2' or clientip match_phrase '4') order by `@timestamp` limit 1; """
        qt_sql """ select * from ${indexTbName1} where (`@timestamp` >= 893964617 and `@timestamp` < 893966455) and (request match_phrase 'hm') order by `@timestamp` limit 1; """
        qt_sql """ select * from ${indexTbName1} where (`@timestamp` >= 893964617 and `@timestamp` < 893966455) and (clientip match_phrase '1' or clientip match_phrase '3') order by `@timestamp` limit 1; """

        qt_sql """ select * from ${indexTbName2} where (request match_phrase 'hm') order by `@timestamp` limit 1; """
        qt_sql """ select * from ${indexTbName2} where (request match_phrase 'hm' and clientip match_phrase '1') order by `@timestamp` limit 1; """
        qt_sql """ select * from ${indexTbName2} where (request match_phrase 'hm' and clientip match_phrase '1') or (request match_phrase 'bg' and clientip match_phrase '2') order by `@timestamp` limit 1; """
        qt_sql """ select * from ${indexTbName2} where (request match_phrase 'hm' and clientip match_phrase '1' or clientip match_phrase '3') or (request match_phrase 'bg' and clientip match_phrase '2' or clientip match_phrase '4') order by `@timestamp` limit 1; """
        qt_sql """ select * from ${indexTbName2} where (`@timestamp` >= 893964617 and `@timestamp` < 893966455) and (request match_phrase 'hm') order by `@timestamp` limit 1; """
        qt_sql """ select * from ${indexTbName2} where (`@timestamp` >= 893964617 and `@timestamp` < 893966455) and (clientip match_phrase '1' or clientip match_phrase '3') order by `@timestamp` limit 1; """
      } finally {
        GetDebugPoint().disableDebugPointForAllBEs("segment_iterator.topn_opt_1")
      }

      try {
        GetDebugPoint().enableDebugPointForAllBEs("segment_iterator.topn_opt_2")

        qt_sql """ select * from ${indexTbName1} where (`@timestamp` >= 893964617 and `@timestamp` < 893966455) and (request match_phrase 'hm' and request like '%ag%') order by `@timestamp` limit 1; """
        qt_sql """ select * from ${indexTbName1} where (`@timestamp` >= 893964617 and `@timestamp` < 893966455) and (request match_phrase 'hm' and clientip like '%1%') order by `@timestamp` limit 1; """
        qt_sql """ select * from ${indexTbName1} where (`@timestamp` >= 893964617 and `@timestamp` < 893966455) and (clientip match_phrase '1' or clientip match_phrase '3' and request like '%ag%') order by `@timestamp` limit 1; """
        qt_sql """ select * from ${indexTbName1} where (`@timestamp` >= 893964617 and `@timestamp` < 893966455) and (clientip match_phrase '1' or `@timestamp` = 1) order by `@timestamp` limit 1; """

        qt_sql """ select * from ${indexTbName2} where (`@timestamp` >= 893964617 and `@timestamp` < 893966455) and (request match_phrase 'hm' and request like '%ag%') order by `@timestamp` limit 1; """
        qt_sql """ select * from ${indexTbName2} where (`@timestamp` >= 893964617 and `@timestamp` < 893966455) and (request match_phrase 'hm' and clientip like '%1%') order by `@timestamp` limit 1; """
        qt_sql """ select * from ${indexTbName2} where (`@timestamp` >= 893964617 and `@timestamp` < 893966455) and (clientip match_phrase '1' or clientip match_phrase '3' and request like '%ag%') order by `@timestamp` limit 1; """
        qt_sql """ select * from ${indexTbName2} where (`@timestamp` >= 893964617 and `@timestamp` < 893966455) and (clientip match_phrase '1' or `@timestamp` = 1) order by `@timestamp` limit 1; """
        
        qt_sql """ select * from ${indexTbName3} where (`@timestamp` >= 893964617 and `@timestamp` < 893966455) and clientip = '34.0.0.0' order by `@timestamp` limit 1; """
        qt_sql """ select * from ${indexTbName3} where clientip = '34.0.0.0' order by `@timestamp` limit 1; """
      } finally {
        GetDebugPoint().disableDebugPointForAllBEs("segment_iterator.topn_opt_2")
      }
    } finally {
    }
}