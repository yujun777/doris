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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

class SimplifyCaseWhenLikeTest extends ExpressionRewriteTestHelper {

    @Test
    void testCaseWhen() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(SimplifyCaseWhenLike.INSTANCE)
        ));
        assertRewriteAfterTypeCoercion("case when a > 1 then a + 1 when a > 1 then a + 10 when a > 2 then a + 2 else a + 100 end",
                "case when a > 1 then a + 1 when a > 2 then a + 2 else a + 100 end");
        assertRewriteAfterTypeCoercion("case when a > 1 then a + 1 when a > 2 then a + 1 when a > 3 then a + 1 else a + 1 end",
                "a + 1");
        assertRewriteAfterTypeCoercion("case when a > 1 then a + 1 when a > 2 then a + 1 when a > 3 then a + 1 end",
                "case when a > 1 then a + 1 when a > 2 then a + 1 when a > 3 then a + 1 end");
        assertRewriteAfterTypeCoercion("case when null then 1 when false then 2 when a > 3 then 3 when a > 4 then 4 end",
                "case when a > 3 then 3 when a > 4 then 4 end");
        assertRewriteAfterTypeCoercion("case when null then 1 when false then 2 when a > 3 then 3 when true then 0 when a > 4 then 4 end",
                "if(a > 3, 3, 0)");
        assertRewriteAfterTypeCoercion("case when true then 100 when a > 1 then a + 1 when a > 1 then a + 10 when a > 2 then a + 2 else a + 100 end",
                "100");
    }

    @Test
    void testIf() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(SimplifyCaseWhenLike.INSTANCE)
        ));
        assertRewriteAfterTypeCoercion("if(true, a + 1,  a + 2)", "a + 1");
        assertRewriteAfterTypeCoercion("if(false, a + 1,  a + 2)", "a + 2");
        assertRewriteAfterTypeCoercion("if(b > 0, a + 100,  a + 100)", "a + 100");
    }

}
