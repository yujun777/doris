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

/**
 * Describes the capabilities of a {@link Stream} implementation.
 * Used by the IVM planner to decide which delta strategies are viable.
 */
public class StreamCapability {
    private final boolean supportsBeforeImage;
    private final boolean supportsDelete;
    private final boolean supportsUpdate;
    private final boolean supportsSnapshotAtCursor;
    private final boolean supportsStableRowIdentity;
    private final boolean appendOnly;

    public StreamCapability(boolean supportsBeforeImage, boolean supportsDelete,
            boolean supportsUpdate, boolean supportsSnapshotAtCursor,
            boolean supportsStableRowIdentity, boolean appendOnly) {
        this.supportsBeforeImage = supportsBeforeImage;
        this.supportsDelete = supportsDelete;
        this.supportsUpdate = supportsUpdate;
        this.supportsSnapshotAtCursor = supportsSnapshotAtCursor;
        this.supportsStableRowIdentity = supportsStableRowIdentity;
        this.appendOnly = appendOnly;
    }

    public boolean isSupportsBeforeImage() {
        return supportsBeforeImage;
    }

    public boolean isSupportsDelete() {
        return supportsDelete;
    }

    public boolean isSupportsUpdate() {
        return supportsUpdate;
    }

    public boolean isSupportsSnapshotAtCursor() {
        return supportsSnapshotAtCursor;
    }

    public boolean isSupportsStableRowIdentity() {
        return supportsStableRowIdentity;
    }

    public boolean isAppendOnly() {
        return appendOnly;
    }

    @Override
    public String toString() {
        return "StreamCapability{"
                + "supportsBeforeImage=" + supportsBeforeImage
                + ", supportsDelete=" + supportsDelete
                + ", supportsUpdate=" + supportsUpdate
                + ", supportsSnapshotAtCursor=" + supportsSnapshotAtCursor
                + ", supportsStableRowIdentity=" + supportsStableRowIdentity
                + ", appendOnly=" + appendOnly
                + '}';
    }
}
