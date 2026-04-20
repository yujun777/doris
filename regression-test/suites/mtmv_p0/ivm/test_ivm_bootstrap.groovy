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

import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite("test_ivm_bootstrap") {

    // =========================================================
    // TC-9-1: BUILD DEFERRED + manual REFRESH (AUTO) on INIT MV
    //         Should skip IVM and perform full refresh successfully.
    // =========================================================
    sql """drop materialized view if exists mv_ivm_bootstrap_auto;"""
    sql """drop table if exists t_ivm_bootstrap_base;"""

    sql """
        CREATE TABLE t_ivm_bootstrap_base (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO t_ivm_bootstrap_base VALUES
            (1, 10),
            (2, 20),
            (3, 30);
    """

    sql """
        CREATE MATERIALIZED VIEW mv_ivm_bootstrap_auto
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT * FROM t_ivm_bootstrap_base;
    """

    // Verify MV is in INIT state
    def mvInfos = sql """
        select RefreshState from mv_infos('database'='${context.dbName}')
        where Name = 'mv_ivm_bootstrap_auto'
    """
    logger.info("mv_infos after create: " + mvInfos.toString())
    assertTrue(mvInfos.toString().contains("INIT"),
            "Expected RefreshState=INIT after BUILD DEFERRED, got: " + mvInfos)

    // Manual REFRESH (default = AUTO mode) on INIT MV should skip IVM → full refresh
    sql """REFRESH MATERIALIZED VIEW mv_ivm_bootstrap_auto AUTO"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_bootstrap_auto")

    // Verify data is correct after bootstrap full refresh
    order_qt_bootstrap_auto_data """SELECT k1, v1 FROM mv_ivm_bootstrap_auto"""

    // Verify RefreshState is now SUCCESS
    def mvInfosAfter = sql """
        select RefreshState from mv_infos('database'='${context.dbName}')
        where Name = 'mv_ivm_bootstrap_auto'
    """
    assertTrue(mvInfosAfter.toString().contains("SUCCESS"),
            "Expected RefreshState=SUCCESS after first refresh, got: " + mvInfosAfter)

    // =========================================================
    // TC-9-2: After bootstrap, REFRESH AUTO should use IVM path
    //         (insert new data, then incremental refresh).
    // =========================================================
    sql """INSERT INTO t_ivm_bootstrap_base VALUES (4, 40), (5, 50);"""

    sql """REFRESH MATERIALIZED VIEW mv_ivm_bootstrap_auto AUTO"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_bootstrap_auto")

    order_qt_bootstrap_auto_after_ivm """SELECT k1, v1 FROM mv_ivm_bootstrap_auto"""

    // =========================================================
    // TC-9-3: BUILD DEFERRED + explicit REFRESH INCREMENTAL on INIT MV
    //         Should let IVM attempt incremental refresh (not preemptively error).
    // =========================================================
    sql """drop materialized view if exists mv_ivm_bootstrap_incr;"""

    sql """
        CREATE MATERIALIZED VIEW mv_ivm_bootstrap_incr
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT * FROM t_ivm_bootstrap_base;
    """

    // Verify INIT state
    def incrMvInfos = sql """
        select RefreshState from mv_infos('database'='${context.dbName}')
        where Name = 'mv_ivm_bootstrap_incr'
    """
    assertTrue(incrMvInfos.toString().contains("INIT"),
            "Expected RefreshState=INIT, got: " + incrMvInfos)

    // INCREMENTAL on INIT MV should let IVM try
    sql """REFRESH MATERIALIZED VIEW mv_ivm_bootstrap_incr INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_bootstrap_incr")

    order_qt_bootstrap_incr_on_init """SELECT k1, v1 FROM mv_ivm_bootstrap_incr"""

    def incrMvInfosAfterIncr = sql """
        select RefreshState from mv_infos('database'='${context.dbName}')
        where Name = 'mv_ivm_bootstrap_incr'
    """
    assertTrue(incrMvInfosAfterIncr.toString().contains("SUCCESS"),
            "Expected RefreshState=SUCCESS after INCREMENTAL, got: " + incrMvInfosAfterIncr)

    // =========================================================
    // TC-9-4: After successful INCREMENTAL, REFRESH COMPLETE should also work.
    // =========================================================
    sql """REFRESH MATERIALIZED VIEW mv_ivm_bootstrap_incr COMPLETE"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_bootstrap_incr")

    order_qt_bootstrap_incr_after_complete """SELECT k1, v1 FROM mv_ivm_bootstrap_incr"""

    def incrMvInfosAfterComplete = sql """
        select RefreshState from mv_infos('database'='${context.dbName}')
        where Name = 'mv_ivm_bootstrap_incr'
    """
    assertTrue(incrMvInfosAfterComplete.toString().contains("SUCCESS"),
            "Expected RefreshState=SUCCESS after COMPLETE, got: " + incrMvInfosAfterComplete)

    // =========================================================
    // TC-9-5: BUILD IMMEDIATE should bootstrap via COMPLETE automatically.
    // =========================================================
    sql """drop materialized view if exists mv_ivm_bootstrap_immediate;"""

    sql """
        CREATE MATERIALIZED VIEW mv_ivm_bootstrap_immediate
        BUILD IMMEDIATE REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT * FROM t_ivm_bootstrap_base;
    """
    waitingMTMVTaskFinishedByMvName("mv_ivm_bootstrap_immediate")

    // Verify data after BUILD IMMEDIATE
    order_qt_bootstrap_immediate_data """SELECT k1, v1 FROM mv_ivm_bootstrap_immediate"""

    // Verify RefreshState is SUCCESS
    def immMvInfos = sql """
        select RefreshState from mv_infos('database'='${context.dbName}')
        where Name = 'mv_ivm_bootstrap_immediate'
    """
    assertTrue(immMvInfos.toString().contains("SUCCESS"),
            "Expected RefreshState=SUCCESS after BUILD IMMEDIATE, got: " + immMvInfos)

    // After BUILD IMMEDIATE, INCREMENTAL refresh should work
    sql """INSERT INTO t_ivm_bootstrap_base VALUES (6, 60);"""

    sql """REFRESH MATERIALIZED VIEW mv_ivm_bootstrap_immediate INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_bootstrap_immediate")

    order_qt_bootstrap_immediate_after_ivm """SELECT k1, v1 FROM mv_ivm_bootstrap_immediate"""
}
