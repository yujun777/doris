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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite ("projectMV1") {
    sql "SET experimental_enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql """ DROP TABLE IF EXISTS projectMV1; """

    sql """
            create table projectMV1 (
                time_col dateV2, 
                empid int, 
                name varchar, 
                deptno int, 
                salary int, 
                commission int)
            partition by range (time_col) (partition p1 values less than MAXVALUE) distributed by hash(time_col) buckets 3 properties('replication_num' = '1');
        """


    sql """insert into projectMV1 values("2020-01-01",1,"a",1,1,1);"""
    sql """insert into projectMV1 values("2020-01-02",2,"b",2,2,2);"""

    createMV("create materialized view projectMV1_mv as select deptno, empid from projectMV1 order by deptno;")

    sleep(3000)

    sql """insert into projectMV1 values("2020-01-01",1,"a",1,1,1);"""

    sql "analyze table projectMV1 with sync;"
    sql """alter table projectMV1 modify column time_col set stats ('row_count'='3');"""

    sql """set enable_stats=false;"""

    mv_rewrite_fail("select * from projectMV1 where time_col='2020-01-01' order by empid;", "projectMV1_mv")
    order_qt_select_star "select * from projectMV1 order by empid;"

    mv_rewrite_success("select empid, deptno from projectMV1 where deptno=0 order by empid;", "projectMV1_mv")
    order_qt_select_mv "select empid, deptno from projectMV1 order by empid;"

    sql """set enable_stats=true;"""

    mv_rewrite_fail("select * from projectMV1 where time_col='2020-01-01' order by empid;", "projectMV1_mv")

    mv_rewrite_success("select empid, deptno from projectMV1 where deptno=0 order by empid;", "projectMV1_mv")
}
