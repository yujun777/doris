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

import org.apache.doris.common.AnalysisException;

/**
 * Abstraction over a change stream for one base table.
 *
 * <p>Implementations are provided per connector (OLAP binlog, Paimon, Iceberg).
 * The IVM planner uses this interface to query stream state and build
 * delta scan specifications.
 */
public interface Stream {
    /** Returns the stream type (OLAP, PAIMON, ICEBERG). */
    StreamType getType();

    /** Returns the capabilities of this stream. */
    StreamCapability getCapability();

    /** Returns the latest available cursor position. */
    StreamCursor getLatestCursor() throws AnalysisException;

    /**
     * Returns true if binlog / change feed data exists in the given range.
     *
     * @param fromExclusive start cursor (exclusive), null for beginning
     * @param toInclusive end cursor (inclusive)
     */
    boolean hasBinlog(StreamCursor fromExclusive, StreamCursor toInclusive) throws AnalysisException;

    /**
     * Creates a specification for building a stream TVF relation in the plan,
     * reading changes from {@code fromExclusive} to {@code toInclusive}.
     *
     * @param fromExclusive start cursor (exclusive), null for beginning
     * @param toInclusive end cursor (inclusive)
     */
    StreamRelationSpec createRelationSpec(StreamCursor fromExclusive,
            StreamCursor toInclusive) throws AnalysisException;

    /**
     * Returns a snapshot of the base table at the given cursor position.
     * Used to pin non-driving tables during multi-table IVM.
     */
    IVMTableSnapshot snapshotAt(StreamCursor cursor) throws AnalysisException;
}
