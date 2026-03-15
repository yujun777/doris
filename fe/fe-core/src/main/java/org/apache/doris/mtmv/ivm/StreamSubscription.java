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
 * A subscription to a {@link Stream} on behalf of a materialized view.
 * Tracks the committed and readable cursor positions.
 *
 * <p>Cursor progress is managed by the stream implementation, not by MTMV metadata.
 */
public interface StreamSubscription {
    /** Unique identifier for this subscription / consumer. */
    String consumerId();

    /** Returns the underlying stream. */
    Stream getStream();

    /**
     * Returns the last committed cursor (i.e., the position up to which
     * changes have been successfully applied to the MV).
     * Returns null if no cursor has been committed yet.
     */
    StreamCursor getCommittedCursor() throws AnalysisException;

    /**
     * Returns the latest cursor position that is safe to read up to.
     * This may be ahead of the committed cursor.
     */
    StreamCursor getReadableCursor() throws AnalysisException;

    /**
     * Commits the given cursor, indicating that all changes up to and
     * including this position have been successfully applied.
     */
    void commitCursor(StreamCursor cursor) throws AnalysisException;
}
