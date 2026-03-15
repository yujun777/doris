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

import java.util.Map;

/**
 * Specification for building a stream table-valued function relation in the
 * query plan. Produced by {@link Stream#createRelationSpec}.
 */
public class StreamRelationSpec {
    private final BaseTableId tableId;
    private final String functionName;
    private final StreamCursor fromExclusive;
    private final StreamCursor toInclusive;
    private final Map<String, String> properties;

    public StreamRelationSpec(BaseTableId tableId, String functionName,
            StreamCursor fromExclusive, StreamCursor toInclusive,
            Map<String, String> properties) {
        this.tableId = tableId;
        this.functionName = functionName;
        this.fromExclusive = fromExclusive;
        this.toInclusive = toInclusive;
        this.properties = properties;
    }

    public BaseTableId getTableId() {
        return tableId;
    }

    public String getFunctionName() {
        return functionName;
    }

    public StreamCursor getFromExclusive() {
        return fromExclusive;
    }

    public StreamCursor getToInclusive() {
        return toInclusive;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "StreamRelationSpec{"
                + "tableId=" + tableId
                + ", functionName='" + functionName + '\''
                + ", from=" + (fromExclusive != null ? fromExclusive.asDebugString() : "null")
                + ", to=" + (toInclusive != null ? toInclusive.asDebugString() : "null")
                + '}';
    }
}
