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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.nereids.trees.plans.Plan;

/**
 * Analyzes a materialized view's rewritten plan tree and classifies it
 * into one of the supported {@link IVMPlanPattern} values.
 */
public interface IVMPlanAnalyzer {
    /**
     * Analyze the given MV plan and return the classification result.
     *
     * @param mvPlan the rewritten MV plan (from MTMVCache)
     * @return analysis result containing the pattern and optional unsupported reason
     */
    IVMPlanAnalysis analyze(Plan mvPlan);
}
