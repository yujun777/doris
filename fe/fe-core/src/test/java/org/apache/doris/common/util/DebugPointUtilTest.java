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

package org.apache.doris.common.util;

import org.apache.doris.common.Config;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.Assert;
import org.junit.Test;

public class DebugPointUtilTest extends TestWithFeService {

    @Test
    public void testDebugPoint() throws Exception {
        Config.enable_debug_points = true;

        Assert.assertFalse(DebugPointUtil.isEnable("dbug1"));
        Assert.assertNotNull(getSqlStmtExecutor("CREATE DEBUG POINT dbug1"));
        Assert.assertTrue(DebugPointUtil.isEnable("dbug1"));
        Assert.assertNotNull(getSqlStmtExecutor("DROP DEBUG POINT dbug1"));
        Assert.assertFalse(DebugPointUtil.isEnable("dbug1"));

        Assert.assertNotNull(getSqlStmtExecutor("CREATE DEBUG POINT dbug2"));
        Assert.assertTrue(DebugPointUtil.isEnable("dbug2"));
        Assert.assertNotNull(getSqlStmtExecutor("CLEAN DEBUG POINT"));
        Assert.assertFalse(DebugPointUtil.isEnable("dbug2"));

        Assert.assertNotNull(getSqlStmtExecutor("CREATE DEBUG POINT dbug3 PROPERTIES (\"excute\"=\"3\")"));
        for (int i = 0; i < 3; i++) {
            Assert.assertTrue(DebugPointUtil.isEnable("dbug3"));
        }
        Assert.assertFalse(DebugPointUtil.isEnable("dbug3"));

        Assert.assertNotNull(getSqlStmtExecutor("CREATE DEBUG POINT dbug4 PROPERTIES (\"timeout\"=\"1\")"));
        Thread.sleep(200);
        Assert.assertTrue(DebugPointUtil.isEnable("dbug4"));
        Thread.sleep(1000);
        Assert.assertFalse(DebugPointUtil.isEnable("dbug4"));
    }
}
