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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mtmv.MTMVRefreshContext;

/**
 * Builds an {@link IVMRefreshContext} for one incremental refresh execution.
 *
 * <p>Responsibilities:
 * <ol>
 *   <li>Read the MV definition and produce the runtime rewritten plan</li>
 *   <li>Read base-table-to-stream bindings from {@link IVMInfo}</li>
 *   <li>Determine a stable base table order</li>
 *   <li>Construct the {@link IVMRefreshContext} without opening subscriptions
 *       or performing delta planning</li>
 * </ol>
 */
public interface IVMRefreshContextBuilder {

    /**
     * Builds the IVM refresh context.
     *
     * @param mtmv the materialized view
     * @param ivmInfo the persistent IVM metadata
     * @param baseContext the base refresh context from MTMVTask
     * @return a new IVM refresh context ready for plan analysis and delta planning
     * @throws AnalysisException if the context cannot be built
     */
    IVMRefreshContext build(
            MTMV mtmv,
            IVMInfo ivmInfo,
            MTMVRefreshContext baseContext) throws AnalysisException;
}
