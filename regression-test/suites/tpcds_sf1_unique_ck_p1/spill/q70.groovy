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
suite("q70_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set spill_min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_ck_p1;
  """
  qt_q70 """
SELECT
  sum(ss_net_profit) total_sum
, s_state
, s_county
, (GROUPING (s_state) + GROUPING (s_county)) lochierarchy
, rank() OVER (PARTITION BY (GROUPING (s_state) + GROUPING (s_county)), (CASE WHEN (GROUPING (s_county) = 0) THEN s_state END) ORDER BY sum(ss_net_profit) DESC) rank_within_parent
FROM
  store_sales
, date_dim d1
, store
WHERE (d1.d_month_seq BETWEEN 1200 AND (1200 + 11))
   AND (d1.d_date_sk = ss_sold_date_sk)
   AND (s_store_sk = ss_store_sk)
   AND (s_state IN (
   SELECT s_state
   FROM
     (
      SELECT
        s_state s_state
      , rank() OVER (PARTITION BY s_state ORDER BY sum(ss_net_profit) DESC) ranking
      FROM
        store_sales
      , store
      , date_dim
      WHERE (d_month_seq BETWEEN 1200 AND (1200 + 11))
         AND (d_date_sk = ss_sold_date_sk)
         AND (s_store_sk = ss_store_sk)
      GROUP BY s_state
   )  tmp1
   WHERE (ranking <= 5)
))
GROUP BY ROLLUP (s_state, s_county)
ORDER BY lochierarchy DESC, (CASE WHEN (lochierarchy = 0) THEN s_state END) ASC, rank_within_parent ASC
LIMIT 100
"""
}
