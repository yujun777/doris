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

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.Map;

/**
 * Persistent IVM metadata stored on each MTMV.
 * Serialized via GSON as part of MTMV's edit-log / image persistence.
 */
public class IVMInfo {
    /** True while an incremental refresh is in progress; used for crash recovery. */
    @SerializedName("ir")
    private boolean inIncrementalRefresh;

    /** Set to true when a stream reports it can no longer serve changes. */
    @SerializedName("bb")
    private boolean binlogBroken;

    /** Per-base-table stream bindings. */
    @SerializedName("bs")
    private Map<BaseTableId, IVMStreamRef> baseTableStreams;

    // For deserialization
    public IVMInfo() {
        this.baseTableStreams = Maps.newHashMap();
    }

    public boolean isInIncrementalRefresh() {
        return inIncrementalRefresh;
    }

    public void setInIncrementalRefresh(boolean inIncrementalRefresh) {
        this.inIncrementalRefresh = inIncrementalRefresh;
    }

    public boolean isBinlogBroken() {
        return binlogBroken;
    }

    public void setBinlogBroken(boolean binlogBroken) {
        this.binlogBroken = binlogBroken;
    }

    public Map<BaseTableId, IVMStreamRef> getBaseTableStreams() {
        return baseTableStreams;
    }

    public void setBaseTableStreams(Map<BaseTableId, IVMStreamRef> baseTableStreams) {
        this.baseTableStreams = baseTableStreams;
    }

    @Override
    public String toString() {
        return "IVMInfo{"
                + "inIncrementalRefresh=" + inIncrementalRefresh
                + ", binlogBroken=" + binlogBroken
                + ", baseTableStreams=" + baseTableStreams
                + '}';
    }
}
