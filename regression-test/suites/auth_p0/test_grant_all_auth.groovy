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

import org.junit.Assert;

suite("test_grant_all_auth","p0,auth") {
    String suiteName = "test_grant_all_auth"
    String user = "${suiteName}_user"
    String pwd = 'C123_567p'
    try_sql("DROP USER ${user}")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant all on *.*.* to ${user}"""
    def res = sql """ show grants for ${user} """
    logger.info("res: " + res.toString())
    assertTrue(res.toString().contains("Select_priv"))
    assertTrue(res.toString().contains("Load_priv"))
    assertTrue(res.toString().contains("Alter_priv"))
    assertTrue(res.toString().contains("Create_priv"))
    assertTrue(res.toString().contains("Drop_priv"))
    assertTrue(res.toString().contains("Show_view_priv"))
    try_sql("DROP USER ${user}")
}